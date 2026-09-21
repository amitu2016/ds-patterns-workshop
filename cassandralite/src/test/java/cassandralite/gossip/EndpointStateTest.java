package cassandralite.gossip;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class EndpointStateTest {

    @Test
    void maxVersionComputesHighestVersionAcrossHeartbeatAndApplicationStates() {
        HeartBeatState hb = new HeartBeatState(1, 5);
        EndpointState state = new EndpointState(hb)
                .withApplicationState(ApplicationState.STATUS, new VersionedValue("NORMAL", 3))
                .withApplicationState(ApplicationState.TOKENS, new VersionedValue("100,200", 8));

        assertEquals(8, state.maxVersion());

        state = state.withHeartBeatState(new HeartBeatState(1, 12));
        assertEquals(12, state.maxVersion());
    }

    @Test
    void statesGreaterThanReturnsOnlyNewerEntries() {
        HeartBeatState hb = new HeartBeatState(1, 10);
        EndpointState state = new EndpointState(hb)
                .withApplicationState(ApplicationState.STATUS, new VersionedValue("NORMAL", 4))
                .withApplicationState(ApplicationState.TOKENS, new VersionedValue("100,200", 12));

        EndpointState delta = state.statesGreaterThan(8);
        assertNotNull(delta);
        assertEquals(10, delta.heartBeatState().version());
        assertNull(delta.getApplicationState(ApplicationState.STATUS));
        assertEquals(new VersionedValue("100,200", 12), delta.getApplicationState(ApplicationState.TOKENS));

        // When nothing is greater, returns null
        assertNull(state.statesGreaterThan(15));
    }

    @Test
    void mergePreservesHighestGenerationsAndVersions() {
        EndpointState state1 = new EndpointState(new HeartBeatState(1, 5))
                .withApplicationState(ApplicationState.STATUS, new VersionedValue("JOINING", 2))
                .withApplicationState(ApplicationState.DC, new VersionedValue("us-east", 1));

        EndpointState state2 = new EndpointState(new HeartBeatState(1, 8))
                .withApplicationState(ApplicationState.STATUS, new VersionedValue("NORMAL", 6))
                .withApplicationState(ApplicationState.LOAD, new VersionedValue("10.5", 4));

        EndpointState merged = state1.merge(state2);

        assertEquals(8, merged.heartBeatState().version());
        assertEquals("NORMAL", merged.getApplicationState(ApplicationState.STATUS).value());
        assertEquals(6, merged.getApplicationState(ApplicationState.STATUS).version());
        assertEquals("us-east", merged.getApplicationState(ApplicationState.DC).value());
        assertEquals("10.5", merged.getApplicationState(ApplicationState.LOAD).value());
    }

    @Test
    void higherGenerationWinsOverLowerGenerationRegardlessOfVersion() {
        EndpointState oldNode = new EndpointState(new HeartBeatState(1, 5000));
        EndpointState rebootedNode = new EndpointState(new HeartBeatState(2, 1));

        EndpointState merged = oldNode.merge(rebootedNode);
        assertEquals(2, merged.heartBeatState().generation());
        assertEquals(1, merged.heartBeatState().version());
    }
}
