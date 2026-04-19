from confluent_kafka.admin import AdminClient, NewTopic
import pandas as pd

def newtopic(topic):
    admin = AdminClient({"bootstrap.servers":"kafka-1:29092,kafka-2:29093,kafka-3:29094"})
    
    newtopic = NewTopic(topic=topic,
                        num_partitions=9,
                        replication_factor=3,
                        config={"cleanup.policy":"delete"})
    
    create = admin.create_topics([newtopic])
    
    for name, items in create.items():
        try:
            items.result()
            print(f"topik {name} berhasil dibuat\n")
        except Exception as e:
            print(f"topik {name} gagal dibuat, dengan error : \n {e}\n")