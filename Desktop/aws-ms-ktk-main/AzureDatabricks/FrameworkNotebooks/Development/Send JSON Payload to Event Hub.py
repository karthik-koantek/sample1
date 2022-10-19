# Databricks notebook source
# MAGIC %md # Send JSON Payload to Event Hub
# MAGIC
# MAGIC Generates sample JSON Messages and saves in an Event Hub.  This is primarily used to generate data for end to end testing of streaming pipelines.
# MAGIC
# MAGIC #### Usage
# MAGIC Supply the parameters and run the notebook.
# MAGIC
# MAGIC Note that setting the timeoutSeconds to 0 will cause this to run until it is stopped.
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * External System for Event Hub must exist.
# MAGIC
# MAGIC #### Details
# MAGIC * To create EventHubTwitterIngestion, run Powershell script /ApplicationConfiguration/Powershell/Scripts/ExternalSystemSecrets/CreateExternalSecretsForEventHub.ps1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %sh pip install azure-eventhub

# COMMAND ----------

import time, asyncio, os, json, uuid, datetime, random
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text("eventHubExternalSystem", "EventHubCVIngestion")
dbutils.widgets.text("messageCount", "100")
dbutils.widgets.text("sleepSecondsBetweenMessages", ".5")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
eventHubExternalSystem = dbutils.widgets.get("eventHubExternalSystem")
messageCount = int(dbutils.widgets.get("messageCount"))
sleepSecondsBetweenMessages = float(dbutils.widgets.get("sleepSecondsBetweenMessages"))
eventHubConnectionString = dbutils.secrets.get(scope=eventHubExternalSystem, key="EventHubConnectionString")
eventHubName = dbutils.secrets.get(scope=eventHubExternalSystem, key="EventHubName")
eventHubConsumerGroup = dbutils.secrets.get(scope=eventHubExternalSystem, key="EventHubConsumerGroup")

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "eventHubExternalSystem": eventHubExternalSystem,
  "messageCount": messageCount,
  "sleepSecondsBetweenMessages": sleepSecondsBetweenMessages,
  "eventHubConnectionString": eventHubConnectionString,
  "eventHubName": eventHubName,
  "eventHubConsumerGroup": eventHubConsumerGroup
}
parameters = json.dumps(p)
notebookLogGuid = str(uuid.uuid4())
log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

print("Notebook Log Guid: {0}".format(notebookLogGuid))
print("Step Log Guid: {0}".format(stepLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

def generateVehicleMessage(vehicleCount, signalCount):
    eventDict = {}
    payload = []
    for s in range(1,signalCount):
        signal = {}
        signal["id"] = "signal{0}".format(s)
        signal["value"] = random.random()
        signal["timestamp"] = str(datetime.datetime.utcnow())
        payload.append(signal)
    eventDict["vehicleId"] = "vehicle{0}".format(vehicleCount)
    eventDict["driverId"] = "driver{0}".format(vehicleCount)
    eventDict["activityId"] = str(uuid.uuid4())
    eventDict["timestamp"] = str(datetime.datetime.utcnow())
    eventDict["payload"] = payload
    eventData = json.dumps(eventDict)
    return eventData

# COMMAND ----------

async def run(messageCount, signalCount=64):
  async with producer:
    event_data_batch = await producer.create_batch()
    for i in range(messageCount):
      time.sleep(sleepSecondsBetweenMessages)
      message = generateVehicleMessage(i, signalCount)
      event_data = EventData(message)
      try:
        event_data_batch.add(event_data)
      except ValueError:
        await producer.send_batch(event_data_batch)
        event_data_batch = await producer.create_batch()
        event_data_batch.add(event_data)

# COMMAND ----------

producer = EventHubProducerClient.from_connection_string(
  conn_str=eventHubConnectionString,
  eventhub_name=eventHubName
)

loop = asyncio.get_event_loop()
start_time = time.time()
loop.run_until_complete(run(messageCount))
print("Send messages in {} seconds.".format(time.time() - start_time))

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")