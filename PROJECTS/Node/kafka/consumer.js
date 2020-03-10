const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['localhost:9092', 'localhost:9093', 'localhost:9094']
})
 
const consumer = kafka.consumer({ groupId: 'consumer-group' })
const topic = 'animals'

const run = async () => {
  // Consuming
  await consumer.connect()
  
  await consumer.subscribe({ topic })
 
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  })
}
 
run().catch(console.error)