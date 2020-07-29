import boto3

client = boto3.resource('sqs')

class QueueManager:
    def createStandardQueue(self,name, delaySeconds, maximumMessageSize, messageRetentionPeriod, visibilityTimeout):
        try:
            response = client.create_queue(
                QueueName=name,
                Attributes={
                'DelaySeconds':delaySeconds, #number of seconds that delivery of all messages is delayed in the queue
                'MaximumMessageSize':maximumMessageSize, #number of bytes that a message can contain not to be rejected by SQS
                'MessageRetentionPeriod':messageRetentionPeriod, #from 60 seconds to 1209600 seconds (14 days)
                'VisibilityTimeout':visibilityTimeout  #number of seconds since a message is processed by a consumer or another in case of fail before that message is deleted
                #KmsMasterKeyId ='' id of key to encrypt messages
                #KmsDataKeyReusePeriodSeconds = 0 number of seconds to reuse the current master key before calling AWS KMS again
                  }
            )
        except:
            print('Error creating queue')


    def createFIFOQueue(self,name, delaySeconds, maximumMessageSize, messageRetentionPeriod, visibilityTimeout):
        response = client.create_queue(
            QueueName=name,
            DelaySeconds=delaySeconds, #number of seconds that delivery of all messages is delayed in the queue
            MaximumMessageSize=maximumMessageSize, #number of bytes that a message can contain not to be rejected by SQS
            MessageRetentionPeriod=messageRetentionPeriod, #from 60 seconds to 1209600 seconds (14 days)
            VisibilityTimeout=visibilityTimeout,  #number of seconds since a message is processed by a consumer or another in case of fail before that message is deleted
            FifoQueue=True,
            ContentBasedDeduplication=True
            # KmsMasterKeyId ='' id of key to encrypt messages
            # KmsDataKeyReusePeriodSeconds = 0 number of seconds to reuse the current master key before calling AWS KMS again
        )

    def getAllQueueNames(self):
        for queue in client.queues.all():
            print("queue:: ",queue.url)

    def addNewTagToQueue(self):
        print("add new tag to queue")

    def updateTagFromQueue(self):
        print("update new tag to queue")

    def deleteTagFromQueue(self):
        print("delete new tag to queue")

    def sendMessageToStandardQueue(self):
        print("sending message to standard queue")

    def sendMessageToFIFOQueue(self):
        print("sending message to fifo queue")

    def sendMessageWithAttributesToStandardQueue(self):
        print("sending message to standard queue")

    def sendMessageWithAttributesToFIFOQueue(self):
        print("sending message to fifo queue")

    def sendMessageWithTimerToStandardQueue(self):
        print("sending message to standard queue")

    def sendMessageWithTimerToFIFOQueue(self):
        print("sending message to fifo queue")

    def shortPolling(self):
        print("short")

    def longPolling(self):
        print("long")

    def startPoolingForMessages(self):
        print("fetching messages...")

    def deleteMessages(self):
        print("deleting messages")

    def deleteQueue(self):
        print("this queue was deleted")


newQueueG = QueueManager()
newQueueG.createStandardQueue("prueba1","10","1024","60","0")

newQueueG.getAllQueueNames()
#
# print("Type the new queue's name")
# newQueueName = input()
#

#
# #creando una cola standard
# response = client.create_queue(
#     QueueName=''
# )
#



'''
response = client.create_queue(
    QueueName ='colaNumero1',
    Attributes = {
        'DelaySeconds': '1',
        'MaximumMessageSize': '262144',
        'MessageRetentionPeriod': '600'
    }
)

print(response.url)



) List queues
'''

'''
 Delete a queue
'''
