import { Athena } from 'aws-sdk'

export type ParsedResult = { [key: string]: string | number }[]

export type FieldFormatters = { [key: string]: (v: any) => any }

const valueParsers = {
	integer: (v: string) => parseInt(v, 10),
	default: (v: string) => v,
	array: (v: string) => JSON.parse(v) as any[],
} as { [key: string]: (v: string) => any }

export const parseAthenaResult = ({
	ResultSet: { Rows, ResultSetMetadata },
	formatFields,
	skip,
}: {
	ResultSet: Athena.ResultSet
	formatFields?: FieldFormatters
	skip?: number
}): ParsedResult => {
	if (!Rows || !ResultSetMetadata || !ResultSetMetadata.ColumnInfo) {
		return []
	}
	const { ColumnInfo } = ResultSetMetadata
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
				const parseValue = valueParsers[Type] || valueParsers.default
				v = parseValue(v)
			}
			if (formatFields && formatFields[Name]) {
				v = formatFields[Name](v)
			}
			return {
				...result,
				[Name]: v,
			}
		}, {})
	})
}
