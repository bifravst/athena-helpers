import { parseAthenaResult } from './parseAthenaResult'

const ResultSet = {
	Rows: [
		{ Data: [{ VarCharValue: 'date' }, { VarCharValue: 'value' }] },
		{
			Data: [
				{ VarCharValue: '2019-08-01T10:29:54.406Z' },
				{ VarCharValue: '2607' },
			],
		},
		{
			Data: [
				{ VarCharValue: '2019-07-31T08:34:20.765Z' },
				{ VarCharValue: '2046' },
			],
		},
	],
	ResultSetMetadata: {
		ColumnInfo: [
			{
				CatalogName: 'hive',
				SchemaName: '',
				TableName: '',
				Name: 'date',
				Label: 'date',
				Type: 'varchar',
				Precision: 2147483647,
				Scale: 0,
				Nullable: 'UNKNOWN',
				CaseSensitive: true,
			},
			{
				CatalogName: 'hive',
				SchemaName: '',
				TableName: '',
				Name: 'value',
				Label: 'value',
				Type: 'integer',
				Precision: 10,
				Scale: 0,
				Nullable: 'UNKNOWN',
				CaseSensitive: false,
			},
		],
	},
}

describe('parseAthenaResult', () => {
	it('parses an Athena result into an array of values', () => {
		expect(
			parseAthenaResult({
				ResultSet,
				skip: 1,
			}),
		).toEqual([
			{
				date: '2019-08-01T10:29:54.406Z',
				value: 2607,
			},
			{
				date: '2019-07-31T08:34:20.765Z',
				value: 2046,
			},
		])
	})

	it('can accept formatters to customize row formatting', () => {
		expect(
			parseAthenaResult({
				ResultSet,
				formatFields: {
					value: v => parseInt(v, 10) / 1000,
				},
				skip: 1,
			}),
		).toEqual([
			{
				date: '2019-08-01T10:29:54.406Z',
				value: 2.607,
			},
			{
				date: '2019-07-31T08:34:20.765Z',
				value: 2.046,
			},
		])
	})

	it('can parse a DESCRIBE TABLE query', () => {
		expect(
			parseAthenaResult({
				ResultSet: {
					Rows: [
						{
							Data: [
								{
									VarCharValue:
										'reported            \tstruct<acc:struct<ts:string,v:array<float>>,bat:struct<ts:string,v:int>,gps:struct<ts:string,v:struct<acc:float,alt:float,hdg:float,lat:float,lng:float,spd:float>>>\tfrom deserializer   ',
								},
							],
						},
						{
							Data: [
								{
									VarCharValue:
										'timestamp           \ttimestamp           \tfrom deserializer   ',
								},
							],
						},
						{
							Data: [
								{
									VarCharValue:
										'deviceid            \tstring              \tfrom deserializer   ',
								},
							],
						},
					],
					ResultSetMetadata: {
						ColumnInfo: [
							{
								CatalogName: 'hive',
								SchemaName: '',
								TableName: '',
								Name: 'col_name',
								Label: 'col_name',
								Type: 'string',
								Precision: 0,
								Scale: 0,
								Nullable: 'UNKNOWN',
								CaseSensitive: false,
							},
							{
								CatalogName: 'hive',
								SchemaName: '',
								TableName: '',
								Name: 'data_type',
								Label: 'data_type',
								Type: 'string',
								Precision: 0,
								Scale: 0,
								Nullable: 'UNKNOWN',
								CaseSensitive: false,
							},
							{
								CatalogName: 'hive',
								SchemaName: '',
								TableName: '',
								Name: 'comment',
								Label: 'comment',
								Type: 'string',
								Precision: 0,
								Scale: 0,
								Nullable: 'UNKNOWN',
								CaseSensitive: false,
							},
						],
					},
				},
			}),
		).toEqual([
			{
				// eslint-disable-next-line @typescript-eslint/camelcase
				col_name: 'reported',
				comment: 'from deserializer',
				// eslint-disable-next-line @typescript-eslint/camelcase
				data_type:
					'struct<acc:struct<ts:string,v:array<float>>,bat:struct<ts:string,v:int>,gps:struct<ts:string,v:struct<acc:float,alt:float,hdg:float,lat:float,lng:float,spd:float>>>',
			},
			{
				// eslint-disable-next-line @typescript-eslint/camelcase
				col_name: 'timestamp',
				comment: 'from deserializer',
				// eslint-disable-next-line @typescript-eslint/camelcase
				data_type: 'timestamp',
			},
			{
				// eslint-disable-next-line @typescript-eslint/camelcase
				col_name: 'deviceid',
				comment: 'from deserializer',
				// eslint-disable-next-line @typescript-eslint/camelcase
				data_type: 'string',
			},
		])
	})

	it('can parse a result with array values', () => {
		expect(
			parseAthenaResult({
				skip: 1,
				ResultSet: {
					ResultSetMetadata: {
						ColumnInfo: [
							{
								CaseSensitive: true,
								CatalogName: 'hive',
								Label: 'date',
								Name: 'date',
								Nullable: 'UNKNOWN',
								Precision: 2147483647,
								Scale: 0,
								SchemaName: '',
								TableName: '',
								Type: 'varchar',
							},
							{
								CaseSensitive: false,
								CatalogName: 'hive',
								Label: 'value',
								Name: 'value',
								Nullable: 'UNKNOWN',
								Precision: 0,
								Scale: 0,
								SchemaName: '',
								TableName: '',
								Type: 'array',
							},
						],
					},
					Rows: [
						{ Data: [{ VarCharValue: 'date' }, { VarCharValue: 'value' }] },
						{
							Data: [
								{ VarCharValue: '2019-08-05T20:06:23.714Z' },
								{ VarCharValue: '[3.4, 4.2, 4.7]' },
							],
						},
						{
							Data: [
								{ VarCharValue: '2019-08-05T20:06:23.322Z' },
								{ VarCharValue: '[3.4, 4.2, 0.0]' },
							],
						},
						{
							Data: [
								{ VarCharValue: '2019-08-05T20:06:22.949Z' },
								{ VarCharValue: '[3.4, 0.0, 0.0]' },
							],
						},
						{
							Data: [
								{ VarCharValue: '2019-08-01T10:30:20.854Z' },
								{ VarCharValue: '[2.1, 0.6, 2.6]' },
							],
						},
						{
							Data: [
								{ VarCharValue: '2019-08-01T10:30:20.374Z' },
								{ VarCharValue: '[2.1, 0.6, 0.0]' },
							],
						},
						{
							Data: [
								{ VarCharValue: '2019-08-01T10:30:19.623Z' },
								{ VarCharValue: '[2.1, 0.0, 0.0]' },
							],
						},
						{
							Data: [
								{ VarCharValue: '2019-07-31T08:34:19.805Z' },
								{ VarCharValue: '[3.6, 4.5, 6.7]' },
							],
						},
						{
							Data: [
								{ VarCharValue: '2019-07-31T08:34:19.157Z' },
								{ VarCharValue: '[3.6, 4.5, 0.0]' },
							],
						},
						{
							Data: [
								{ VarCharValue: '2019-07-31T08:34:18.597Z' },
								{ VarCharValue: '[3.6, 0.0, 0.0]' },
							],
						},
					],
				},
			}),
		).toEqual([
			{
				date: '2019-08-05T20:06:23.714Z',
				value: [3.4, 4.2, 4.7],
			},
			{
				date: '2019-08-05T20:06:23.322Z',
				value: [3.4, 4.2, 0.0],
			},
			{
				date: '2019-08-05T20:06:22.949Z',
				value: [3.4, 0.0, 0.0],
			},
			{
				date: '2019-08-01T10:30:20.854Z',
				value: [2.1, 0.6, 2.6],
			},
			{
				date: '2019-08-01T10:30:20.374Z',
				value: [2.1, 0.6, 0.0],
			},
			{
				date: '2019-08-01T10:30:19.623Z',
				value: [2.1, 0.0, 0.0],
			},
			{
				date: '2019-07-31T08:34:19.805Z',
				value: [3.6, 4.5, 6.7],
			},
			{
				date: '2019-07-31T08:34:19.157Z',
				value: [3.6, 4.5, 0.0],
			},
			{
				date: '2019-07-31T08:34:18.597Z',
				value: [3.6, 0.0, 0.0],
			},
		])
	})
})
