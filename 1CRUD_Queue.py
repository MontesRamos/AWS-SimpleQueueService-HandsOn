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
#newQueueG.createFIFOQueue("queue1","0","1024","60","0")
#newQueueG.getAllQueueNames()
#queue::>  https://eu-west-1.queue.amazonaws.com/569214539827/prueba1
#queue::>  https://eu-west-1.queue.amazonaws.com/569214539827/queue12.fifo
newQueueG.addNewTagToQueue('https://eu-west-1.queue.amazonaws.com/569214539827/prueba1', 'department','IT')