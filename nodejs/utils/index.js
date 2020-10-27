const db = require('../db')

module.exports = {
  saveItem: async (item) => {
    const col = await db.getCollection()
    item['task_id'] = process.env['CRAWLAB_TASK_ID']
    const isDedup = process.env['CRAWLAB_IS_DEDUP']
    const dedupField = process.env['CRAWLAB_DEDUP_FIELD']
    const dedupMethod = process.env['CRAWLAB_DEDUP_METHOD']
    try {
      let result;
      if (isDedup) {
        if (dedupMethod === 'overwrite') {
          const query = {}
          query[dedupField] = item[dedupField]
          await col.removeOne(query)
          result = await col.insertOne(item)
        } else if (dedupMethod === 'ignore') {
          result = await col.insertOne(item)
        } else {
          result = await col.insertOne(item)
        }
      } else {
        result = await col.insertOne(item)
      }
      return new Promise(resolve => {
        resolve(result);
      });
    } catch (e) {
      return new Promise((_,reject) => {
          reject(e);
      });
    }
  },
  close: async () => {
    const client = await db.getClient()
    await client.close()
  }
}
