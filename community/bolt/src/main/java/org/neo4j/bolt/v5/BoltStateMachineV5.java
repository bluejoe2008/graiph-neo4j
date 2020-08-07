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
package org.neo4j.bolt.v5;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.blob.LaterAccessBlobCache;
import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v3.runtime.ConnectedState;
import org.neo4j.bolt.v3.runtime.FailedState;
import org.neo4j.bolt.v3.runtime.InterruptedState;
import org.neo4j.bolt.v3.runtime.ReadyState;
import org.neo4j.bolt.v3.runtime.StreamingState;
import org.neo4j.bolt.v3.runtime.TransactionReadyState;
import org.neo4j.bolt.v3.runtime.TransactionStreamingState;
import org.neo4j.bolt.v5.runtime.TransactionReadyStateV5;
import org.neo4j.kernel.impl.KernelTransactionEventHub;

import java.time.Clock;

public class BoltStateMachineV5 extends BoltStateMachineV3
{
    public LaterAccessBlobCache _blobCache = new LaterAccessBlobCache(); //<--pandadb->

    public BoltStateMachineV5( BoltStateMachineSPI boltSPI, BoltChannel boltChannel, Clock clock )
    {
        super( boltSPI, boltChannel, clock );
        //<--pandadb->
        KernelTransactionEventHub.addListener(_blobCache);
        //<--pandadb->
    }

    @Override
    protected States buildStates()
    {
        ConnectedState connected = new ConnectedState();
        ReadyState ready = new ReadyState();
        StreamingState streaming = new StreamingState();

        //supoorts blob: TransactionReadyStateV5
        TransactionReadyState txReady = new TransactionReadyStateV5();
        TransactionStreamingState txStreaming = new TransactionStreamingState();
        FailedState failed = new FailedState();
        InterruptedState interrupted = new InterruptedState();

        connected.setReadyState( ready );

        ready.setTransactionReadyState( txReady );
        ready.setStreamingState( streaming );
        ready.setFailedState( failed );
        ready.setInterruptedState( interrupted );

        streaming.setReadyState( ready );
        streaming.setFailedState( failed );
        streaming.setInterruptedState( interrupted );

        txReady.setReadyState( ready );
        txReady.setTransactionStreamingState( txStreaming );
        txReady.setFailedState( failed );
        txReady.setInterruptedState( interrupted );

        txStreaming.setReadyState( txReady );
        txStreaming.setFailedState( failed );
        txStreaming.setInterruptedState( interrupted );

        failed.setInterruptedState( interrupted );

        interrupted.setReadyState( ready );

        return new States( connected, failed );
    }

}
