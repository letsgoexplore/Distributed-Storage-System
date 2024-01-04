## To be discussed
每个node是否按顺序编号？如果是的话，它需要先向某个节点询问一共有多少个节点，它是多少号，这可能会引起分歧？还是说node自己起id，然后进行hash

## Code Structure
- `design/design.md`: illustrating psudocode
- `Config.py`: saving the universal parameter for the system
- `hash_ring.py`: provide the hash consistant algorithm
- `network_layer.py`: define the 'Node' data structure, handling its behaviours
- `data_layer.py`: define the 'Data' data structure, handling its behaviours
- `operation.py`: list all the behaviour the node will be acting, act as state transition trigger in the state machine
- `main.py`: currently no 'main.py', it's now in 'operation.py' for convinience

## How to run
### Start Node
- For the first node:
    - `python operation.py start_root_node`
- For other node:
    - `python operation.py start_node ID IP PORT`

### Store File
- First start the client
  - `python client.py`
  - add node

### Read File

