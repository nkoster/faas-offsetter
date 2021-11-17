module.exports = async (body, res) => {

  const {topic, partition, offset} = body
  // const broker = 'pvdevkafka01:9093'
  const broker = 'kafka-databus.pvdev:9092'

  console.log(topic, partition, offset)

  const { exec } = require('child_process')
  const execute = (cmd, callback) => exec(cmd, (_, stdout) => callback(stdout))
  
  const data = await new Promise((resolve, reject) => {
    try {
      const cmd = `kafkacat -C -b ${broker} -t ${topic} -p ${partition} -o ${offset} -c 1 -e -q`
      execute(cmd, result => resolve(result))
      console.log(cmd)
    } catch (err) {
      console.log(err.message)
      reject({})
    }
  })

  return res.status(200).send(data ? JSON.parse(data) : [])
    
}
