package com.example.kafkadr.cluster;

/**
 * Callback interface for components that need to react to cluster switches.
 */
public interface ClusterSwitchListener {

    /**
     * Called when the active cluster changes.
     *
     * @param previousCluster the cluster we are switching away from (null on initial selection)
     * @param newCluster      the cluster we are switching to
     */
    void onClusterSwitch(ClusterInfo previousCluster, ClusterInfo newCluster);
}
