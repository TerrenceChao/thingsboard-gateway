#     Copyright 2022. MobileDrive
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import os
from sqlite3.dbapi2 import connect
import time

from simplejson import dump
from kafka import KafkaProducer

from thingsboard_gateway.storage.event_storage import EventStorage, log
from thingsboard_gateway.storage.kafka.storage_settings import StorageSettings

from logging import getLogger

log = getLogger("storage")

class KafkaEventStorage(EventStorage):
    def __init__(self, config):
        log.debug("\n\n\n &&& Kafka 初始化中")
        self.__stopped = False
        self.settings = StorageSettings(config)
        self.connect()


    def put(self, event):
        log.debug("\n\n\n &&& Kafka [put]")
        log.debug(event)
        log.debug("&&& Kafka [put]\n\n\n ")
        
        print("\n\n\n &&& Kafka [put]")
        print(event)
        print("&&& Kafka [put]\n\n\n ")

        try:
            if not self.__stopped:
                print("Sending data to storage")
                log.info("Sending data to storage")
                self.send(event)
                return True
            else:
                return False
        except Exception as e:
            self.connect()
            log.exception(e)


    def get_event_pack(self):
        # DO NOT Implement: 批次讀取後將會刪除，但這裡並沒有要和 ThingsBoard 互動，Kafka不需要
        return []

    def event_pack_processing_done(self):
        # DO NOT Implement: 判斷該批次讀取結束後將會刪除，Kafka不需要
        pass

    def stop(self):
        # ...Stop the storage processing
        self.producer.close()
        self.__stopped = True


    def connect(self):
        self.producer = KafkaProducer(bootstrap_servers=self.settings.host)

    def send(self, event):
        self.producer.send(self.settings.topic, event.encode("utf-8"))