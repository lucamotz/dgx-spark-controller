# Glossary

| Term | Meaning |
| --- | --- |
| controller host | the Linux or macOS machine that runs the controller service |
| node | one DGX Spark (or compatible GB10 system) in `nodes.json` |
| controller ID | stable configured node identity such as `spark-a`; not necessarily the host name |
| observed host name | the name the node's OS reports; informational only |
| management address | the node address used for SSH and endpoint URLs |
| cluster address | an address on the direct node-to-node link (usually ConnectX-7) |
| setup mode | the service state without a valid `nodes.json`; lifecycle routes answer `setup_required` |
| setup wizard | the Setup page: scan, host-key trust, identify, link check, save |
| identify | the fixed, read-only SSH probe that classifies a node and fills its configuration |
| host-key pinning | storing a confirmed host key in the controller `known_hosts` and enforcing it with strict checking |
| profile | a versioned, allowlisted contract for running, identifying and stopping one model |
| reviewed profile | a hand-written profile in `<config dir>/profiles/` (`origin: reviewed`) |
| generated profile | a profile written by the recipe builder after a prepare (`origin: generated`, unqualified) |
| qualified | vouched for by a person who tested the profile; never true for generated profiles |
| recipe (draft) | a Hugging Face model, a vLLM release, a placement and bounded settings, saved before prepare |
| prepare | the journaled operation that pulls the image and downloads the weights on every node, then writes a generated profile |
| launch mode | `head-script` (one launcher on the control node) or `per-node` (the controller starts each rank) |
| control node / head | the rank-0 node that serves the endpoint |
| artifact | model identity and required files; not a launch command |
| deployment | one profile placed on its nodes |
| operation | a durable journaled action (`load`, `unload`, `recover`, `reconcile`, `prepare`) with events |
| desired state | the profile and placement an operator requested |
| observed state | facts measured on the node, process and endpoint |
| readiness | process identity + HTTP health + exact model alias |
| managed process | a process or container started or adopted under a known profile identity |
| unmanaged process | anything that does not match a managed identity; never stopped automatically |
| ownership marker | a per-node file naming the deployment the controller started there |
| preflight | read-only checks before any destructive step |
| drain | the bounded period before stopping a deployment |
| recovery threshold | the minimum free memory and maximum swap required after a stop |
| argv fragment | one literal command-line argument, not shell-quoted text |
| lifecycle owner | the only component allowed to start or stop models: the controller |
