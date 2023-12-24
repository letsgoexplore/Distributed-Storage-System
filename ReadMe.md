## Code Structure
- design: illustrating code structure
- network_layer: define the 'Node' data structure, handling its behaviours
- data_layer: define the 'Data' data structure, handling its behaviours
- operation: list all the behaviour the node will be acting, act as state transition trigger in the state machine
- main: currently no 'main.py', it's now in 'operation.py' for convinience

## How to run
### Start Node
- For the first node:
    - `python operation.py start_network`
- For other node:
    - `python operation.py join_network`

### Store File
- Conducting on one node, move the file to the *File* address
- `python operation.py store_data`

### Read File

