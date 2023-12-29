
# Join
```python
ROOT_NODE, ROOT_IP, ROOT_PORT
ip_table=[]
data_table=[]
def join()
    join_request(ROOT_IP, ROOT_PORT, IP, PORT, id)
                                                                    handle_join_request(发送ACK; 更改node_table, data_table)
                                                                        {NodeTable:add_node(data_table, id, ip, port)
                                                                         DataTable:add_node(id)}
    node_table <- request_node_table(ROOT_IP, ROOT_PORT, IP, PORT)
                                                                    handle_request_node_table(发送table)
    data_table <- request_data_table(ROOT_IP, ROOT_PORT, IP, PORT)
                                                                    handle_request_data_table(发送table)
    for node in node_table:
        join_request(node.ip, node.port, IP, PORT)
                                                                    handle_join_request(发送ACK; 更改node_table, data_table)
    [data_id, node] <- Datatable:search_personal_data_from_table(id, data_table)

    for data_id, node in [data_id, node]:
        request_data(node.ip, node.port, IP, PORT, data_id)
                                                                    handle_request_data(发送数据)        
                                         
```

# quit
```python
ROOT_NODE, ROOT_IP, ROOT_PORT
ip_table
data_table
def quit()
    for node in node_table:
        quit_request(node.ip, node.port, IP, PORT)
                                                                    handle_quit_request(返回ACK; 确认哪些是需要自己存储的数据, 更改node_table, data_table)
                                                                        {NodeTable:quit_node(data_table, id, ip, port)
                                                                         DataTable:quit_node(id)}
                                                                        [data_id, node] <- Datatable:quit_data_transmission(id, data_table)
                                                                        }

    

    for data_id, node in [data_id, node]:
        request_data(node.ip, node.port, IP, PORT, data_id)
                                                                    handle_request_data(发送数据)        
                                                
```

# Store
```python
ip_table
data_table
def store():


```



---------- history: when dealing with ACK together -----------
```python
def WaitACK([node], port) -> [node]
    监听port, wait 3s
    if node_1 <- message:
        [node] <- [node]/{node_1}
    return 
```
```python
ROOT_NODE, ROOT_IP, ROOT_PORT
ip_table=[]
data_table=[]
def join()
    join_request(ROOT_IP, ROOT_PORT, IP, PORT, id)
                                                                        handle_join_request(发送ACK; 更改node_table, data_table)
                                                                        NodeTable:add_node(data_table, id, ip, port)
                                                                        DataTable:add_node(id)
    node_table <- request_node_table(ROOT_IP, ROOT_PORT, IP, PORT)
                                                                        handle_request_node_table(发送table)
    data_table <- request_data_table(ROOT_IP, ROOT_PORT, IP, PORT)
                                                                        handle_request_data_table(发送table)
    unreceived_ack_list_init = node_table/{self, root_node} 
    unreceived_ack_list = node_table/{self, root_node} #这是因为在循环遍历的过程中不能改变数组
    while unreceived_ack_list_init ≠ []:                                                               
        for node in unreceived_ack_list:
            join_request(node.ip, node.port, IP, PORT)
        unreceived_ack_list_init = unreceived_ack_list
                                                                        handle_join_request(发送ACK; 更改node_table, data_table)
    [data_id, node] <- Datatable:search_personal_data_from_table(id, data_table)

    unreceived_data_list_init = [data_id, node]
    unreceived_data_list = [data_id, node]
    while unreceived_ack_list ≠ []:
        for data_id, node in unreceived_ack_list:
            request_data(node.ip, node.port, IP, PORT, data_id)
                                                                        handle_request_data(发送数据)
        unreceived_data_list_init = unreceived_data_list
        
                                                
```
