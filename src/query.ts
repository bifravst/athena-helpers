import { Athena } from 'aws-sdk'
import { exponential, Backoff } from 'backoff'

type logFn = (...args: any) => void
const noLog = () => undefined

const queryExecutionInfo = ({
	QueryExecutionId,
	athena,
	debugLog,
	errorLog,
}: {
	debugLog: logFn
	errorLog: logFn
	QueryExecutionId: string
	athena: Athena
}) => {
	const status = async (): Promise<{
		status: 'QUEUED' | 'RUNNING' | 'FAILED' | 'SUCCEEDED'
		result: Athena.Types.GetQueryExecutionOutput
	}> => {
		const result = await athena
			.getQueryExecution({ QueryExecutionId })
			.promise()
		const status =
			(result.QueryExecution &&
				result.QueryExecution.Status &&
				result.QueryExecution.Status.State) ||
			'unknown'

		switch (status) {
			case 'QUEUED':
			case 'RUNNING':
			case 'SUCCEEDED':
				debugLog({
					QueryExecutionId,
					State: status,
				})
				return {
					status,
					result,
				}
			case 'FAILED':
				errorLog({
					QueryExecutionId,
					QueryExecution: result.QueryExecution,
				})
				return {
					status,
					result,
				}
			case 'unknown':
			default:
				errorLog({
					QueryExecutionId,
					QueryExecution: result.QueryExecution,
				})
				throw new Error(
					`Query ${QueryExecutionId} has unexpected status: "${status}"!`,
				)
		}
	}

	const abort = async () =>
		athena
			.stopQueryExecution({
				QueryExecutionId,
			})
			.promise()

	return {
		status,
		abort,
	}
}

const waitForQueryCompletion = async ({
	QueryExecutionId,
	athena,
	debugLog,
	errorLog,
	runningBackoff,
	queuedBackoff,
}: {
	QueryExecutionId: string
	athena: Athena
	debugLog: logFn
	errorLog: logFn
	runningBackoff?: Backoff
	queuedBackoff?: Backoff
}) =>
	new Promise((resolve, reject) => {
		// Wait up to 2 minutes while query is queued
		const q =
			queuedBackoff ||
			(() => {
				const b = exponential({
					randomisationFactor: 0,
					initialDelay: 1000,
					maxDelay: 5000,
				})
				b.failAfter(26) // 117
				return b
			})()

		const { status, abort } = queryExecutionInfo({
			QueryExecutionId,
			athena,
			debugLog,
			errorLog,
		})

		q.on('ready', async () => {
			const e = await status()
			if (e.status === 'QUEUED') {
				q.backoff()
			} else if (e.status === 'SUCCEEDED') {
				resolve(e.result)
			} else {
				// Wait for query result
				const r =
					runningBackoff ||
					(() => {
						const b = exponential({
							randomisationFactor: 0,
							initialDelay: 1000,
							maxDelay: 5000,
						})
						b.failAfter(14) // 62000
						return b
					})()
				r.on('ready', async () => {
					const e = await status()
					switch (e.status) {
						case 'FAILED':
							reject(new Error(`Query ${QueryExecutionId} failed!`))
							break
						case 'SUCCEEDED':
							resolve(e.result)
							break
						default:
							r.backoff()
							break
					}
				})
				r.on('fail', async () => {
					await abort()
					reject(new Error(`Timed out waiting for query ${QueryExecutionId}`))
				})
				r.backoff()
			}
		})
		q.backoff()
		q.on('fail', async () => {
			await abort()
			reject(
				new Error(
					`Timed out waiting for query execution to start ${QueryExecutionId}`,
				),
			)
		})
	})

/**
 * Construct a query executor
 * @param athena an AWS Athena instance
 * @param WorkGroup the name of the Athena WorkGroup
 * @param queuedBackoff an instance of Backoff to use for waiting while the query is queued, if none provided a default one will be used
 * @param runningBackoff an instance of Backoff to use for waiting while the query is running, if none provided a default one will be used
 * @param debugLog provide a method to receive debug logs
 * @param errorLog provide a method to receive error logs
 */
export const query = ({
	athena,
	WorkGroup,
	queuedBackoff,
	runningBackoff,
	debugLog,
	errorLog,
}: {
	athena: Athena
	WorkGroup: string
	queuedBackoff?: Backoff
	runningBackoff?: Backoff
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

		d({ WorkGroup, QueryString: QueryString.trim() })
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

		await waitForQueryCompletion({
			QueryExecutionId,
			athena,
			debugLog: d,
			errorLog: e,
			queuedBackoff,
			runningBackoff,
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
