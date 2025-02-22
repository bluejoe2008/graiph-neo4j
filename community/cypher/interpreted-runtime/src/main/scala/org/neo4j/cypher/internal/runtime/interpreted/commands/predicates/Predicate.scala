/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.CastSupport
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.IsList
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedNodeProperty
import org.neo4j.cypher.operations.CypherBoolean
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.cypher.internal.v3_5.util.CypherTypeException
import org.neo4j.cypher.internal.v3_5.util.NonEmptyList

import scala.util.Failure
import scala.util.Success
import scala.util.Try

abstract class BlobPredicate extends Predicate {

}

abstract class Predicate extends Expression {
  def apply(ctx: ExecutionContext, state: QueryState): Value =
    isMatch(ctx, state).
      map(Values.booleanValue).
      getOrElse(Values.NO_VALUE)

  def isTrue(m: ExecutionContext, state: QueryState): Boolean = isMatch(m, state).getOrElse(false)
  def andWith(other: Predicate): Predicate = Ands(this, other)
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean]

  // This is the un-dividable list of predicates. They can all be ANDed
  // together
  def atoms: Seq[Predicate] = Seq(this)
  def containsIsNull: Boolean

  def andWith(preds: Predicate*): Predicate =
    if (preds.isEmpty) this else preds.fold(this)(_ andWith _)
}

object Predicate {
  def fromSeq(in: Seq[Predicate]) = in.reduceOption(_ andWith _).getOrElse(True())
}

abstract class CompositeBooleanPredicate extends Predicate {

  def predicates: NonEmptyList[Predicate]

  def shouldExitWhen: Boolean

  def symbolTableDependencies: Set[String] =
    predicates.map(_.symbolTableDependencies).reduceLeft(_ ++ _)

  def containsIsNull: Boolean = predicates.exists(_.containsIsNull)

  /**
   * This algorithm handles the case where we combine multiple AND or multiple OR groups (CNF or DNF).
   * As well as performing shortcut evaluation so that a false (for AND) or true (for OR) will exit the
   * evaluation without performing any further predicate evaluations (including those that could throw
   * exceptions). Any exception thrown is held until the end (or until the exit state) so that it is
   * superceded by exit predicates (false for AND and true for OR).
   */
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    predicates.foldLeft[Try[Option[Boolean]]](Success(Some(!shouldExitWhen))) { (previousValue, predicate) =>
      previousValue match {
        // if a previous evaluation was true (false) the OR (AND) result is determined
        case Success(Some(result)) if result == shouldExitWhen => previousValue
        case _ =>
          Try(predicate.isMatch(m, state)) match {
            // If we get the exit case (false for AND and true for OR) ignore any error cases
            case Success(Some(result)) if result == shouldExitWhen => Success(Some(shouldExitWhen))
            // Handle null only for non error cases
            case Success(None) if previousValue.isSuccess => Success(None)
            // errors or non-exit cases propagate as normal
            case Failure(e) if previousValue.isSuccess => Failure(e)
            case _ => previousValue
          }
      }
    } match {
      case Failure(e) => throw e
      case Success(option) => option
    }
  }

  def arguments: Seq[Expression] = predicates.toIndexedSeq

  override def atoms: Seq[Predicate] = predicates.toIndexedSeq
}

case class Not(a: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = a.isMatch(m, state) match {
    case Some(x) => Some(!x)
    case None    => None
  }
  override def toString: String = "NOT(" + a + ")"
  def containsIsNull = a.containsIsNull
  def rewrite(f: (Expression) => Expression) = f(Not(a.rewriteAsPredicate(f)))
  def arguments = Seq(a)
  def symbolTableDependencies = a.symbolTableDependencies
}

case class Xor(a: Predicate, b: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] =
    (a.isMatch(m, state), b.isMatch(m, state)) match {
      case (None, _) => None
      case (_, None) => None
      case (Some(l), Some(r)) => Some(l ^ r)
    }

  override def toString: String = "(" + a + " XOR " + b + ")"
  def containsIsNull = a.containsIsNull || b.containsIsNull
  def rewrite(f: (Expression) => Expression) = f(Xor(a.rewriteAsPredicate(f), b.rewriteAsPredicate(f)))

  def arguments = Seq(a, b)

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class IsNull(expression: Expression) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = expression(m, state) match {
    case Values.NO_VALUE => Some(true)
    case _ => Some(false)
  }

  override def toString: String = expression + " IS NULL"
  def containsIsNull = true
  def rewrite(f: (Expression) => Expression) = f(IsNull(expression.rewrite(f)))
  def arguments = Seq(expression)
  def symbolTableDependencies = expression.symbolTableDependencies
}

case class True() extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = Some(true)
  override def toString: String = "true"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = f(this)
  def arguments = Nil
  def symbolTableDependencies = Set()
}

case class PropertyExists(variable: Expression, propertyKey: KeyToken) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = variable(m, state) match {
    case pc: VirtualNodeValue => Some(propertyKey.getOptId(state.query).exists(state.query.nodeOps.hasProperty(pc.id, _)))
    case pc: VirtualRelationshipValue => Some(
      propertyKey.getOptId(state.query).exists(state.query.relationshipOps.hasProperty(pc.id, _)))
    case IsMap(map) => Some(map(state.query).get(propertyKey.name) != Values.NO_VALUE)
    case Values.NO_VALUE => None
    case _ => throw new CypherTypeException("Expected " + variable + " to be a property container.")
  }

  override def toString: String = s"hasProp($variable.${propertyKey.name})"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(PropertyExists(variable.rewrite(f), propertyKey.rewrite(f)))

  def arguments = Seq(variable)

  def symbolTableDependencies = variable.symbolTableDependencies
}

case class CachedNodePropertyExists(cachedNodeProperty: Expression) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    cachedNodeProperty match {
      case cnp: AbstractCachedNodeProperty =>
        val nodeId = cnp.getNodeId(m)
        if (nodeId == StatementConstants.NO_SUCH_NODE) {
          None
        } else {
          Some(state.query.nodeOps.hasTxStatePropertyForCachedNodeProperty(nodeId, cnp.getPropertyKey(state.query)))
        }
      case _ => throw new CypherTypeException("Expected " + cachedNodeProperty + " to be a cached node property.")
    }
  }

  override def toString: String = s"hasCachedNodeProp($cachedNodeProperty)"

  def containsIsNull = false

  def rewrite(f: Expression => Expression) = f(CachedNodePropertyExists(cachedNodeProperty.rewrite(f)))

  def arguments = Seq(cachedNodeProperty)

  def symbolTableDependencies: Set[String] = cachedNodeProperty.symbolTableDependencies
}

trait StringOperator {
  self: Predicate =>
  override def isMatch(m: ExecutionContext, state: QueryState) = (lhs(m, state), rhs(m, state)) match {
    case (l: TextValue, r: TextValue) => Some(compare(l, r))
    case (_, _) => None
  }

  def lhs: Expression
  def rhs: Expression
  def compare(a: TextValue, b: TextValue): Boolean
  override def containsIsNull = false
  override def arguments = Seq(lhs, rhs)
  override def symbolTableDependencies = lhs.symbolTableDependencies ++ rhs.symbolTableDependencies
}

case class StartsWith(lhs: Expression, rhs: Expression) extends Predicate with StringOperator {
  override def compare(a: TextValue, b: TextValue) = a.startsWith(b)

  override def rewrite(f: (Expression) => Expression) = f(copy(lhs.rewrite(f), rhs.rewrite(f)))
}

case class EndsWith(lhs: Expression, rhs: Expression) extends Predicate with StringOperator {
  override def compare(a: TextValue, b: TextValue) = a.endsWith(b)

  override def rewrite(f: (Expression) => Expression) = f(copy(lhs.rewrite(f), rhs.rewrite(f)))
}

case class Contains(lhs: Expression, rhs: Expression) extends Predicate with StringOperator {
  override def compare(a: TextValue, b: TextValue) = a.contains(b)

  override def rewrite(f: (Expression) => Expression) = f(copy(lhs.rewrite(f), rhs.rewrite(f)))
}

case class LiteralRegularExpression(lhsExpr: Expression, regexExpr: Literal)
                                   (implicit converter: TextValue => TextValue = identity) extends Predicate {
  lazy val pattern = converter(regexExpr.anyVal.asInstanceOf[TextValue]).stringValue().r.pattern

  def isMatch(m: ExecutionContext, state: QueryState) =
    lhsExpr(m, state) match {
      case s: TextValue => Some(pattern.matcher(s.stringValue()).matches())
      case _ => None
    }

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(regexExpr.rewrite(f) match {
    case lit: Literal => LiteralRegularExpression(lhsExpr.rewrite(f), lit)(converter)
    case other        => RegularExpression(lhsExpr.rewrite(f), other)(converter)
  })

  def arguments = Seq(lhsExpr, regexExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ regexExpr.symbolTableDependencies

  override def toString = s"$lhsExpr =~ $regexExpr"
}

case class RegularExpression(lhsExpr: Expression, regexExpr: Expression)
                            (implicit converter: TextValue => TextValue = identity) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val lValue = lhsExpr(m, state)
    val rValue = regexExpr(m, state)
    (lValue, rValue) match {
      case (lhs: TextValue, rhs) if rhs != Values.NO_VALUE =>
        val rhsAsRegexString = converter(CastSupport.castOrFail[TextValue](rhs))
        Some(CypherBoolean.regex(lhs, rhsAsRegexString).booleanValue())
      case _ => None
    }
  }

  override def toString: String = lhsExpr.toString() + " ~= /" + regexExpr.toString() + "/"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(regexExpr.rewrite(f) match {
    case lit:Literal => LiteralRegularExpression(lhsExpr.rewrite(f), lit)(converter)
    case other => RegularExpression(lhsExpr.rewrite(f), other)(converter)
  })

  def arguments = Seq(lhsExpr, regexExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ regexExpr.symbolTableDependencies
}

case class NonEmpty(collection: Expression) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    collection(m, state) match {
      case IsList(x) => Some(x.nonEmpty)
      case x if x == Values.NO_VALUE => None
      case x => throw new CypherTypeException("Expected a collection, got `%s`".format(x))
    }
  }

  override def toString: String = "nonEmpty(" + collection.toString() + ")"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(NonEmpty(collection.rewrite(f)))

  def arguments = Seq(collection)

  def symbolTableDependencies = collection.symbolTableDependencies
}

case class HasLabel(entity: Expression, label: KeyToken) extends Predicate {

  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = entity(m, state) match {

    case Values.NO_VALUE =>
      None

    case value =>
      val node = CastSupport.castOrFail[VirtualNodeValue](value)
      val nodeId = node.id
      val queryCtx = state.query

      label.getOptId(state.query) match {
        case None =>
          Some(false)
        case Some(labelId) =>
          Some(queryCtx.isLabelSetOnNode(labelId, nodeId))
      }
  }

  override def toString = s"$entity:${label.name}"

  def rewrite(f: (Expression) => Expression) = f(HasLabel(entity.rewrite(f), label.typedRewrite[KeyToken](f)))

  override def children = Seq(label, entity)

  def arguments: Seq[Expression] = Seq(entity)

  def symbolTableDependencies = entity.symbolTableDependencies ++ label.symbolTableDependencies

  def containsIsNull = false
}

case class CoercedPredicate(inner: Expression) extends Predicate {
  def arguments = Seq(inner)

  def isMatch(m: ExecutionContext, state: QueryState) = inner(m, state) match {
    case x: BooleanValue => Some(x.booleanValue())
    case Values.NO_VALUE => None
    case IsList(coll) => Some(coll.nonEmpty)
    case x => throw new CypherTypeException(s"Don't know how to treat that as a predicate: $x")
  }

  def rewrite(f: (Expression) => Expression) = f(CoercedPredicate(inner.rewrite(f)))

  def containsIsNull = false

  def symbolTableDependencies = inner.symbolTableDependencies

  override def toString = inner.toString
}
