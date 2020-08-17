/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v3_5.parser

import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.expressions
import org.neo4j.cypher.internal.v3_5.{expressions => ast}
import org.parboiled.scala._

import scala.collection.mutable.ListBuffer

trait Expressions extends Parser
  with Literals
  with Patterns
  with Base {

  // Precedence loosely based on http://en.wikipedia.org/wiki/Operators_in_C_and_C%2B%2B#Operator_precedence

  def Expression = Expression12

  def Expression12: Rule1[ast.Expression] = rule("an expression") {
    Expression11 ~ zeroOrMore(WS ~ (
      group(keyword("OR") ~~ Expression11) ~~>> (Or(_: ast.Expression, _))
      ): ReductionRule1[ast.Expression, ast.Expression])
  }

  def Expression11: Rule1[ast.Expression] = rule("an expression") {
    Expression10 ~ zeroOrMore(WS ~ (
      group(keyword("XOR") ~~ Expression10) ~~>> (expressions.Xor(_: ast.Expression, _))
      ): ReductionRule1[ast.Expression, ast.Expression])
  }

  def Expression10: Rule1[ast.Expression] = rule("an expression") {
    Expression9 ~ zeroOrMore(WS ~ (
      group(keyword("AND") ~~ Expression9) ~~>> (And(_: ast.Expression, _))
      ): ReductionRule1[ast.Expression, ast.Expression])
  }

  def Expression9: Rule1[ast.Expression] = rule("an expression")(
    group(keyword("NOT") ~~ Expression9) ~~>> (expressions.Not(_))
      | Expression8
  )

  def Expression8: Rule1[ast.Expression] = rule("comparison expression") {
    val produceComparisons: (ast.Expression, List[PartialComparison]) => InputPosition => ast.Expression = comparisons
    Expression7 ~ zeroOrMore(WS ~ PartialComparisonExpression) ~~>> produceComparisons
  }

  case class PartialComparison(op: (ast.Expression, ast.Expression) => (InputPosition) => ast.Expression,
                               expr: ast.Expression, pos: InputPosition) {
    def apply(lhs: ast.Expression) = op(lhs, expr)(pos)
  }

  def PartialComparisonExpression: Rule1[PartialComparison] = (
    group(operator("=") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(eq, expr, pos) }
      | group(operator("~") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(eqv, expr, pos) }
      | group(operator("<>") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(ne, expr, pos) }
      | group(operator("!=") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(bne, expr, pos) }
      | group(operator("<") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(lt, expr, pos) }
      | group(operator(">") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(gt, expr, pos) }
      | group(operator("<=") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(lte, expr, pos) }
      | group(operator(">=") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(gte, expr, pos) })

  def eq(lhs: ast.Expression, rhs: ast.Expression): InputPosition => ast.Expression = expressions.Equals(lhs, rhs)

  def eqv(lhs: ast.Expression, rhs: ast.Expression): InputPosition => ast.Expression = ast.Equivalent(lhs, rhs)

  def ne(lhs: ast.Expression, rhs: ast.Expression): InputPosition => ast.Expression = expressions.NotEquals(lhs, rhs)

  def bne(lhs: ast.Expression, rhs: ast.Expression): InputPosition => ast.Expression = expressions.InvalidNotEquals(lhs, rhs)

  def lt(lhs: ast.Expression, rhs: ast.Expression): InputPosition => ast.Expression = expressions.LessThan(lhs, rhs)

  def gt(lhs: ast.Expression, rhs: ast.Expression): InputPosition => ast.Expression = expressions.GreaterThan(lhs, rhs)

  def lte(lhs: ast.Expression, rhs: ast.Expression): InputPosition => ast.Expression = expressions.LessThanOrEqual(lhs, rhs)

  def gte(lhs: ast.Expression, rhs: ast.Expression): InputPosition => ast.Expression = expressions.GreaterThanOrEqual(lhs, rhs)

  def comparisons(first: ast.Expression, rest: List[PartialComparison]): InputPosition => ast.Expression = {
    rest match {
      case Nil => _ => first
      case second :: Nil => _ => second(first)
      case more =>
        var lhs = first
        val result = ListBuffer.empty[ast.Expression]
        for (rhs <- more) {
          result.append(rhs(lhs))
          lhs = rhs.expr
        }
        Ands(Set(result: _*))
    }
  }

  def Expression7: Rule1[ast.Expression] = rule("an expression") {
    Expression6 ~ zeroOrMore(WS ~ (
      group(operator("+") ~~ Expression6) ~~>> (ast.Add(_: ast.Expression, _))
        | group(operator("-") ~~ Expression6) ~~>> (ast.Subtract(_: ast.Expression, _))
      ))
  }

  def Expression6: Rule1[ast.Expression] = rule("an expression") {
    Expression5 ~ zeroOrMore(WS ~ (
      group(operator("*") ~~ Expression5) ~~>> (ast.Multiply(_: ast.Expression, _))
        | group(operator("/") ~~ Expression5) ~~>> (ast.Divide(_: ast.Expression, _))
        | group(operator("%") ~~ Expression5) ~~>> (ast.Modulo(_: ast.Expression, _))
      ))
  }

  def Expression5: Rule1[ast.Expression] = rule("an expression") {
    Expression4 ~ zeroOrMore(WS ~ (
      group(operator("^") ~~ Expression4) ~~>> (ast.Pow(_: ast.Expression, _))
      ))
  }

  def Expression4: Rule1[ast.Expression] = rule("an expression")(
    Expression3
      | group(operator("+") ~~ Expression4) ~~>> (ast.UnaryAdd(_))
      | group(operator("-") ~~ Expression4) ~~>> (ast.UnarySubtract(_))
  )

  def Expression3: Rule1[org.neo4j.cypher.internal.v3_5.expressions.Expression] = rule("an expression") {
    Expression2 ~ zeroOrMore(WS ~ (
      group(operator("=~") ~~ Expression2) ~~>> (expressions.RegexMatch(_: org.neo4j.cypher.internal.v3_5.expressions.Expression, _))
        | group(keyword("IN") ~~ Expression2) ~~>> (expressions.In(_: org.neo4j.cypher.internal.v3_5.expressions.Expression, _))
        | group(keyword("STARTS WITH") ~~ Expression2) ~~>> (expressions.StartsWith(_: org.neo4j.cypher.internal.v3_5.expressions.Expression, _))
        | group(keyword("ENDS WITH") ~~ Expression2) ~~>> (expressions.EndsWith(_: org.neo4j.cypher.internal.v3_5.expressions.Expression, _))
        | group(keyword("CONTAINS") ~~ Expression2) ~~>> (expressions.Contains(_: org.neo4j.cypher.internal.v3_5.expressions.Expression, _))
        | keyword("IS NULL") ~~>> (expressions.IsNull(_: org.neo4j.cypher.internal.v3_5.expressions.Expression))
        | keyword("IS NOT NULL") ~~>> (expressions.IsNotNull(_: org.neo4j.cypher.internal.v3_5.expressions.Expression))
      ): ReductionRule1[org.neo4j.cypher.internal.v3_5.expressions.Expression, org.neo4j.cypher.internal.v3_5.expressions.Expression])
  }

  def Expression2: Rule1[org.neo4j.cypher.internal.v3_5.expressions.Expression] = rule("an expression") {
    Expression1 ~ zeroOrMore(WS ~ (
      PropertyLookup
        | NodeLabels ~~>> (ast.HasLabels(_: org.neo4j.cypher.internal.v3_5.expressions.Expression, _))
        | "[" ~~ Expression ~~ "]" ~~>> (ast.ContainerIndex(_: org.neo4j.cypher.internal.v3_5.expressions.Expression, _))
        | "[" ~~ optional(Expression) ~~ ".." ~~ optional(Expression) ~~ "]" ~~>> (ast.ListSlice(_: org.neo4j.cypher.internal.v3_5.expressions.Expression, _, _))
      ))
  }

  def Expression1: Rule1[ast.Expression] = rule("an expression")(
    NumberLiteral
      | StringLiteral
      | Parameter
      | keyword("TRUE") ~ push(ast.True()(_))
      | keyword("FALSE") ~ push(ast.False()(_))
      | keyword("NULL") ~ push(ast.Null()(_))
      | CaseExpression
      | group(keyword("COUNT") ~~ "(" ~~ "*" ~~ ")") ~ push(ast.CountStar()(_))
      | MapLiteral
      | MapProjection
      | ListComprehension
      | PatternComprehension
      | group("[" ~~ zeroOrMore(Expression, separator = CommaSep) ~~ "]") ~~>> (ast.ListLiteral(_))
      | group(keyword("FILTER") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.FilterExpression(_, _, _))
      | group(keyword("EXTRACT") ~~ "(" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ ")") ~~>> (ast.ExtractExpression(_, _, _, _))
      | group(keyword("REDUCE") ~~ "(" ~~ Variable ~~ "=" ~~ Expression ~~ "," ~~ IdInColl ~~ "|" ~~ Expression ~~ ")") ~~>> (ast.ReduceExpression(_, _, _, _, _))
      | group(keyword("ALL") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.AllIterablePredicate(_, _, _))
      | group(keyword("ANY") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.AnyIterablePredicate(_, _, _))
      | group(keyword("NONE") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.NoneIterablePredicate(_, _, _))
      | group(keyword("SINGLE") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.SingleIterablePredicate(_, _, _))
      | ShortestPathPattern ~~> expressions.ShortestPathExpression
      | RelationshipsPattern ~~> PatternExpression
      | parenthesizedExpression
      | FunctionInvocation
      | Variable
  )

  def parenthesizedExpression: Rule1[ast.Expression] = "(" ~~ Expression ~~ ")"

  def PropertyExpression: Rule1[org.neo4j.cypher.internal.v3_5.expressions.Property] = rule {
    Expression1 ~ oneOrMore(WS ~ PropertyLookup)
  }

  def PropertyLookup: ReductionRule1[ast.Expression, org.neo4j.cypher.internal.v3_5.expressions.Property] = rule("'.'") {
    operator(".") ~~ (PropertyKeyName ~~>> (ast.Property(_: ast.Expression, _)))
  }

  def FilterExpression: Rule3[Variable, ast.Expression, Option[ast.Expression]] =
    IdInColl ~ optional(WS ~ keyword("WHERE") ~~ Expression)

  def IdInColl: Rule2[Variable, ast.Expression] =
    Variable ~~ keyword("IN") ~~ Expression

  def FunctionInvocation: Rule1[org.neo4j.cypher.internal.v3_5.expressions.FunctionInvocation] = rule("a function") {
    ((group(Namespace ~~ FunctionName ~~ "(" ~~
      (keyword("DISTINCT") ~ push(true) | EMPTY ~ push(false)) ~~
      zeroOrMore(Expression, separator = CommaSep) ~~ ")"
    ) ~~> (_.toIndexedSeq)) memoMismatches) ~~>> (ast.FunctionInvocation(_, _, _, _))
  }

  def ListComprehension: Rule1[org.neo4j.cypher.internal.v3_5.expressions.ListComprehension] = rule("[") {
    group("[" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ "]") ~~>> (ast.ListComprehension(_, _, _, _))
  }

  def PatternComprehension: Rule1[ast.PatternComprehension] = rule("[") {
    group("[" ~~ optional(Variable ~~ operator("=")) ~~ RelationshipsPattern ~ optional(WS ~ keyword("WHERE") ~~ Expression) ~~ "|" ~~ Expression ~~ "]") ~~>> (
      (a, b, c, d) => pos => ast.PatternComprehension(a, b, c, d)(pos, Set.empty))
  }

  def CaseExpression: Rule1[org.neo4j.cypher.internal.v3_5.expressions.CaseExpression] = rule("CASE") {
    (group((
      keyword("CASE") ~~ push(None) ~ oneOrMore(WS ~ CaseAlternatives)
        | keyword("CASE") ~~ Expression ~~> (Some(_)) ~ oneOrMore(WS ~ CaseAlternatives)
      ) ~ optional(WS ~
      keyword("ELSE") ~~ Expression
    ) ~~ keyword("END")
    ) memoMismatches) ~~>> (ast.CaseExpression(_, _, _))
  }

  def CaseAlternatives: Rule2[ast.Expression, ast.Expression] = rule("WHEN") {
    keyword("WHEN") ~~ Expression ~~ keyword("THEN") ~~ Expression
  }
}
