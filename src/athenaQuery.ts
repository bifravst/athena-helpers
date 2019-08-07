import { Athena } from 'aws-sdk'
import { exponential, Backoff } from 'backoff'

type logFn = (...args: any) => void
const noLog = () => undefined

/**
 * Construct a query executor
 * @param athena an AWS Athena instance
 * @param WorkGroup the name of the Athena WorkGroup
 * @param backoff an instance of Backoff, if none provided a default one will be used
 * @param debugLog provide a method to receive debug logs
 * @param errorLog provide a method to receive error logs
 */
export const athenaQuery = ({
	athena,
	WorkGroup,
	backoff,
	debugLog,
	errorLog,
}: {
	athena: Athena
	WorkGroup: string
	backoff?: Backoff
	debugLog?: logFn | undefined
	errorLog?: logFn | undefined
}) =>
	/**
	 * Execute a query
	 * @param QueryString the query string
	 */
	async ({
		QueryString,
	}: {
		QueryString: string
	}): Promise<Athena.ResultSet> => {
		const d: logFn = debugLog || noLog
		const e: logFn = errorLog || noLog
		const b =
			backoff ||
			(() => {
				const b = exponential({
					randomisationFactor: 0,
					initialDelay: 1000,
					maxDelay: 5000,
				})
				b.failAfter(8) // 32000
				return b
			})()

		d({ QueryString })
		const { QueryExecutionId } = await athena
			.startQueryExecution({
				WorkGroup,
				QueryString,
			})
			.promise()
		if (!QueryExecutionId) {
			throw new Error(`Query failed!`)
		}
		d({ QueryExecutionId })

		await new Promise((resolve, reject) => {
			b.on('ready', async () => {
				const res = await athena
					.getQueryExecution({ QueryExecutionId })
					.promise()
				const State =
					(res.QueryExecution &&
						res.QueryExecution.Status &&
						res.QueryExecution.Status.State) ||
					'unknown'

				d({
					QueryExecutionId,
					State,
				})

				switch (State) {
					case 'RUNNING':
						b.backoff()
						break
					case 'FAILED':
						console.error(res.QueryExecution)
						e({
							QueryExecutionId,
							QueryExecution: res.QueryExecution,
						})
						reject(new Error(`Query ${QueryExecutionId} failed!`))
						break
					case 'SUCCEEDED':
						resolve(res)
						break
					case 'unknown':
					default:
						e({
							QueryExecutionId,
							QueryExecution: res.QueryExecution,
						})
						reject(
							new Error(`Query ${QueryExecutionId} has unexpected status!`),
						)
				}
			})
			b.on('fail', () => {
				reject(new Error(`Timed out waiting for query ${QueryExecutionId}`))
			})
			b.backoff()
		})

		const { ResultSet } = await athena
			.getQueryResults({ QueryExecutionId })
			.promise()

		if (!ResultSet || !ResultSet.Rows) {
			e({
				QueryExecutionId,
				ResultSet,
			})
			throw new Error(`No resultset returned.`)
		}
		d({
			QueryExecutionId,
			ResultSet,
		})
		return ResultSet
	}
