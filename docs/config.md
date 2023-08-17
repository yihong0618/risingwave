# Configurations

*This doc is meant for development purposes, it may not be most update to date, some things here are meant for internal use only.*

In our system, we have several types of configurations:

1. Node-level Configurations
2. System Configurations
3. Session Configurations

## Command Line Configurations

The command line configurations are used to configure the system at the node level.

You may take a look at the root level `lib.rs` for our main system components, in their `start` entrypoint functions:
- [Frontend](../src/frontend/src/lib.rs)
- [Compute](../src/compute/src/lib.rs)
- [Meta](../src/meta/src/lib.rs)

The argument they take refers to the node-level configurations.
These node-level configurations are largely static, such as the node's IP address, the port it listens to, etc.
System configurations are passed in as part of node-level configurations to the meta-node. They will be managed by the meta
node, and can be changed at runtime.

## System Level configurations

To see how System Configs work, take a look at the [User Documentation](https://www.risingwave.dev/docs/current/view-configure-system-parameters/).

These configurations are maintain by the meta service, and will be supplied by it to the other nodes.

For instance, on compute node (CN) startup, meta node will supply the CN with the state_store_url, which it will use
to initialize its state store.

## Session level configurations

Session level configurations are configurations that are specific to a session. They are supplied by the user when
they create a session.

As an example:

```sql
SET RW_ENABLE_TWO_PHASE_AGG = false;
select min(v1), sum(v2) from t; -- This query will not use two phase aggregation.
```

Note that some of these session variables are experimental. Please refer to [User Documentation](https://www.risingwave.dev)
to see what is supported.