import { Athena } from 'aws-sdk'

export type ParsedResult = { [key: string]: string | number }[]

export type Formatters = { [key: string]: (v: string) => any }

const defaultFormatters = {
	integer: (v: string) => parseInt(v, 10),
	default: (v: string) => v,
	array: (v: string) => JSON.parse(v) as any[],
}

export const parseAthenaResult = ({
	ResultSet: { Rows, ResultSetMetadata },
	formatters,
	skip,
}: {
	ResultSet: Athena.ResultSet
	formatters?: Formatters
	skip?: number
}): ParsedResult => {
	if (!Rows || !ResultSetMetadata || !ResultSetMetadata.ColumnInfo) {
		return []
	}
	const { ColumnInfo } = ResultSetMetadata
	const f = {
		...defaultFormatters,
		...formatters,
	} as Formatters
	return Rows.slice(skip).map(({ Data }) => {
		if (!Data) {
			return {}
		}
		return ColumnInfo.reduce((result, { Name, Type }, key) => {
			let v
			if (Data.length !== ColumnInfo.length && Data.length === 1) {
				// tab-separated
				v = (Data[0].VarCharValue as string).split('\t').map(t => t.trim())[key]
			} else {
				v = Data[key].VarCharValue
			}
			if (v !== undefined) {
				const formatter = f[Type] || f.default
				v = formatter(v)
			}
			return {
				...result,
				[Name]: v,
			}
		}, {})
	})
}
