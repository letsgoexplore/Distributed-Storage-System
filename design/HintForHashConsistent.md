在Python中，有几种方式可以生成哈希值：

1. **内置的 `hash()` 函数**：适用于哈希不可变类型（如字符串和数字）。这个函数的输出取决于运行时的Python版本和其他因素，因此不适用于跨Python会话的持久化存储。

   ```python
   hash_value = hash("your_string")
   ```

2. **`hashlib` 模块**：提供了一系列的加密哈希函数，如MD5, SHA1, SHA256等，适用于生成一个固定大小的哈希值。

   ```python
   import hashlib

   hash_object = hashlib.sha256(b'your_string')
   hex_dig = hash_object.hexdigest()
   ```

3. **`uuid` 模块**：生成唯一的标识符，虽然不是严格意义上的哈希函数，但在需要唯一标识时非常有用。

   ```python
   import uuid

   unique_id = uuid.uuid4()
   ```

关于一致性哈希算法（Consistent Hashing Algorithm）的实现，Python标准库中没有直接提供，但有一些第三方库可以实现这个功能。一个流行的选择是`hash_ring`库。

安装`hash_ring`：

```sh
pip install hash_ring
```

使用`hash_ring`：

```python
from hash_ring import HashRing

members = ['node1', 'node2', 'node3']
ring = HashRing(members)

node = ring.get_node('your_string')
```

在这个例子中，`HashRing`对象会根据提供的节点列表创建一个一致性哈希环。然后你可以使用`get_node`方法来根据字符串（例如一个键）找到它应该映射到的节点。

请注意，一致性哈希通常用于分布式系统中，如负载均衡或数据分区。在选择具体实现时，需要考虑你的具体需求和应用场景。