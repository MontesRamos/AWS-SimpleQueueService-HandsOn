import boto3

client = boto3.resource('sqs')
client2 = boto3.client('sqs')

class QueueManager:
    MINIMUM_DELAY_SECOND_VALUE=0
    MAXIMUM_DELAY_SECOND_VALUE = 900

    MINIMUM_DELAY_MAXIMUMMESSAGESIZE_VALUE = 1024
    MAXIMUM_DELAY_MAXIMUMMESSAGESIZE_VALUE = 262144

    MINIMUM_DELAY_MessageRetentionPeriod_VALUE = 60
    MAXIMUM_DELAY_MessageRetentionPeriod_VALUE = 1209600

    MINIMUM_DELAY_VisibilityTimeout_VALUE = 0
    MAXIMUM_DELAY_VisibilityTimeout_VALUE = 43200

    @staticmethod
    def validateRange(target, minimumValue, maximumValue):
        ok=False
        if(float(target) >= float(minimumValue) and float(target) <= float(maximumValue)):
            ok= True
        else:
            print("Target value: " + target + " needs to be between " + str(minimumValue) + " and " + str(maximumValue))
        return ok


    def createStandardQueue(self,name, delaySeconds, maximumMessageSize, messageRetentionPeriod, visibilityTimeout):
        ok = QueueManager.validateRange(delaySeconds, QueueManager.MINIMUM_DELAY_SECOND_VALUE,QueueManager.MAXIMUM_DELAY_SECOND_VALUE) and \
             QueueManager.validateRange(maximumMessageSize, QueueManager.MINIMUM_DELAY_MAXIMUMMESSAGESIZE_VALUE,QueueManager.MAXIMUM_DELAY_MAXIMUMMESSAGESIZE_VALUE) and \
             QueueManager.validateRange(messageRetentionPeriod, QueueManager.MINIMUM_DELAY_MessageRetentionPeriod_VALUE,QueueManager.MAXIMUM_DELAY_MessageRetentionPeriod_VALUE) and \
             QueueManager.validateRange(visibilityTimeout, QueueManager.MINIMUM_DELAY_VisibilityTimeout_VALUE,QueueManager.MAXIMUM_DELAY_VisibilityTimeout_VALUE)
        if ok:
            try:

                response = client.create_queue(
                    QueueName=name,
                    Attributes={
                    'DelaySeconds':delaySeconds,# number of seconds that delivery of all messages is delayed in the queue
                    'MaximumMessageSize':maximumMessageSize,# number of bytes that a message can contain not to be rejected by SQS
                    'MessageRetentionPeriod':messageRetentionPeriod,  # from 60 seconds to 1209600 seconds (14 days)
                    'VisibilityTimeout':visibilityTimeout}
                    # number of seconds since a message is processed by a consumer or another in case of fail before that message is deleted
                    # KmsMasterKeyId ='' id of key to encrypt messages
                    # KmsDataKeyReusePeriodSeconds = 0 number of seconds to reuse the current master key before calling AWS KMS again
                )
            except:
                print("Error creating FIFO queue")

        else:
            print("Invalid parameters")


    def createFIFOQueue(self,name, delaySeconds, maximumMessageSize, messageRetentionPeriod, visibilityTimeout):
        ok = QueueManager.validateRange(delaySeconds, QueueManager.MINIMUM_DELAY_SECOND_VALUE,QueueManager.MAXIMUM_DELAY_SECOND_VALUE) and \
             QueueManager.validateRange(maximumMessageSize, QueueManager.MINIMUM_DELAY_MAXIMUMMESSAGESIZE_VALUE,QueueManager.MAXIMUM_DELAY_MAXIMUMMESSAGESIZE_VALUE) and \
             QueueManager.validateRange(messageRetentionPeriod, QueueManager.MINIMUM_DELAY_MessageRetentionPeriod_VALUE,QueueManager.MAXIMUM_DELAY_MessageRetentionPeriod_VALUE) and \
             QueueManager.validateRange(visibilityTimeout, QueueManager.MINIMUM_DELAY_VisibilityTimeout_VALUE,QueueManager.MAXIMUM_DELAY_VisibilityTimeout_VALUE)
        if ok:
            try:
                response = client.create_queue(
                    QueueName=name+".fifo",
                    Attributes={
                        'DelaySeconds': delaySeconds,# number of seconds that delivery of all messages is delayed in the queue
                        'MaximumMessageSize': maximumMessageSize,# number of bytes that a message can contain not to be rejected by SQS
                        'MessageRetentionPeriod': messageRetentionPeriod,# from 60 seconds to 1209600 seconds (14 days)
                        'VisibilityTimeout': visibilityTimeout,
                        'FifoQueue':'True',
                        'ContentBasedDeduplication':'True'
                    }
                    # KmsMasterKeyId ='' id of key to encrypt messages
                    # KmsDataKeyReusePeriodSeconds = 0 number of seconds to reuse the current master key before calling AWS KMS again
                )
            except Exception as e:
                print(e)

        else:

            print("Invalid parameters")

    def getAllQueueNames(self):
        print("Current queues:")
        print("------------------------------------")
        for queue in client.queues.all():
            print("queue::> ",queue.url)
        print("------------------------------------")

    def addNewTagToQueue(self, queueUrl, tagName, tagValue):
        response=client2.tag_queue(
            QueueUrl=queueUrl,
            Tags={
               tagName:tagValue
            }
        )

    def deleteTagFromQueue(self, queueUrl, tagName):
        response = client2.untag_queue(
            QueueUrl=queueUrl,
            Tags=[
                tagName
            ]
        )

    def makeShortPollingCallWay1(self,queueUrl): #Change ReceiveMessageWaitTimeSeconds to 0 = More expensive

        response = client2.set_queue_attributes(
            QueueUrl=queueUrl,
            Attributes={
                'ReceiveMessageWaitTimeSeconds': str(0) #to 0
            }
        )

        response = client2.receive_message(
            QueueUrl=queueUrl,
            AttributeNames=[
                'All'
            ],
            MessageAttributeNames=[
                'string',
            ],
            MaxNumberOfMessages=10,
            VisibilityTimeout=120
            #WaitTimeSeconds=0, #this is not specified
        )

        for messages in response:
            print(messages)

    def makeShortPollingCallWay2(self, queueUrl):  # Change ReceiveMessageWaitTimeSeconds to 0 = More expensive
        response = client2.receive_message(
            QueueUrl=queueUrl,
            AttributeNames=[
                'All'
            ],
            MessageAttributeNames=[
                'string',
            ],
            MaxNumberOfMessages=10,
            VisibilityTimeout=120,
            WaitTimeSeconds=0, #this!!!
        )

    def makeLongPollingCall(self,queueUrl,timePollingValue): #Change ReceiveMessageWaitTimeSeconds between 1 and 20 = Less expensive
        response = client2.receive_message(
            QueueUrl=queueUrl,
            AttributeNames=[
                'All'
            ],
            MessageAttributeNames=[
                'string',
            ],
            MaxNumberOfMessages=10,
            VisibilityTimeout=120,
            WaitTimeSeconds=timePollingValue #this needs to be at least 1!!!
        )

    def knowIfQueueHasShortOrLongPolling(self, queueUrl):
        response = client2.get_queue_attributes(
            QueueUrl=queueUrl,
            AttributeNames=[
                'ReceiveMessageWaitTimeSeconds'
            ]
        )
        print(response)

    def getQueueAttributes(self, queueUrl):
        response = client2.get_queue_attributes(
            QueueUrl=queueUrl,
            AttributeNames=[
                'Policy' , 'VisibilityTimeout' , 'MaximumMessageSize' , 'MessageRetentionPeriod' , 'ApproximateNumberOfMessages' , 'ApproximateNumberOfMessagesNotVisible' , 'CreatedTimestamp' , 'LastModifiedTimestamp' , 'QueueArn' , 'ApproximateNumberOfMessagesDelayed' , 'DelaySeconds' ,'ReceiveMessageWaitTimeSeconds' , 'RedrivePolicy' ,  'KmsMasterKeyId' , 'KmsDataKeyReusePeriodSeconds',
            ]
        )
        print(response.body)

    def deleteQueue(self, queueUrl):
        response = client2.delete_queue(
            QueueUrl=queueUrl
        )

    def sendMessageToStandardQueue(self,queueUrl, message, delaySeconds):
        response = client2.send_message(
            QueueUrl=queueUrl,
            MessageBody=str(message),
            DelaySeconds=int(delaySeconds)
        )

    def sendMessageToFIFOQueue(self):
        response = client2.send_message(
            QueueUrl='string',
            MessageBody='string',
            DelaySeconds=123,
            MessageAttributes={
                'Author': {
                        'StringValue': 'Ruben',
                        'DataType': 'String'
                        }
            },
            MessageSystemAttributes={
                'string': {
                    'StringValue': 'string',
                    'BinaryValue': b'bytes',
                    'StringListValues': [
                        'string',
                    ],
                    'BinaryListValues': [
                        b'bytes',
                    ],
                    'DataType': 'string'
                }
            },
            MessageDeduplicationId='string',
            MessageGroupId='string'
        )

    def sendMessageWithAttributesToStandardQueue(self):
        print("sending message to standard queue")

    def sendMessageWithAttributesToFIFOQueue(self):
        print("sending message to fifo queue")

    def sendMessageWithTimerToStandardQueue(self):
        print("sending message to standard queue")

    def sendMessageWithTimerToFIFOQueue(self):
        print("sending message to fifo queue")

    def deleteMessages(self):
        print("deleting messages")

newQueueG = QueueManager()
#newQueueG.createFIFOQueue("queue1","0","1024","60","0")
#newQueueG.getAllQueueNames()
#queue::>  https://eu-west-1.queue.amazonaws.com/569214539827/prueba1
#queue::>  https://eu-west-1.queue.amazonaws.com/569214539827/queue12.fifo
#newQueueG.addNewTagToQueue('https://eu-west-1.queue.amazonaws.com/569214539827/prueba1', 'department','IT')
#newQueueG.knowIfQueueHasShortOrLongPolling('https://eu-west-1.queue.amazonaws.com/569214539827/prueba1')

# newQueueG.sendMessageToStandardQueue('https://eu-west-1.queue.amazonaws.com/569214539827/prueba1', 'hola!', 5)
# newQueueG.sendMessageToStandardQueue('https://eu-west-1.queue.amazonaws.com/569214539827/prueba1', 'hola!', 5)
# newQueueG.sendMessageToStandardQueue('https://eu-west-1.queue.amazonaws.com/569214539827/prueba1', 'hola!', 5)
newQueueG.makeShortPollingCallWay1('https://eu-west-1.queue.amazonaws.com/569214539827/prueba1')
#newQueueG.knowIfQueueHasShortOrLongPolling('https://eu-west-1.queue.amazonaws.com/569214539827/prueba1')
#newQueueG.makeShortPollingCallWay2('https://eu-west-1.queue.amazonaws.com/569214539827/prueba1')
