import {
	ArrayFieldType,
	ScalarFieldType,
	StructFieldType,
	createTableSQL,
} from './createTableSQL'

describe('Athena SQL generator', () => {
	describe('createTableSQL', () => {
		it('should create the proper SQL', () => {
			expect(
				createTableSQL({
					database: 'fooDatabase',
					table: 'barTable',
					s3Location: 's3://bazBucket/',
					fields: {
						reported: {
							type: StructFieldType.struct,
							fields: {
								acc: {
									type: StructFieldType.struct,
									fields: {
										ts: {
											type: ScalarFieldType.bigint,
										},
										v: {
											type: ArrayFieldType.array,
											items: ScalarFieldType.float,
										},
									},
								},
								bat: {
									type: StructFieldType.struct,
									fields: {
										ts: {
											type: ScalarFieldType.bigint,
										},
										v: {
											type: ScalarFieldType.int,
										},
									},
								},
								gps: {
									type: StructFieldType.struct,
									fields: {
										ts: {
											type: ScalarFieldType.bigint,
										},
										v: {
											type: StructFieldType.struct,
											fields: {
												acc: {
													type: ScalarFieldType.float,
												},
												alt: {
													type: ScalarFieldType.float,
												},
												hdg: {
													type: ScalarFieldType.float,
												},
												lat: {
													type: ScalarFieldType.float,
												},
												lng: {
													type: ScalarFieldType.float,
												},
												spd: {
													type: ScalarFieldType.float,
												},
											},
										},
									},
								},
							},
						},
						timestamp: {
							type: ScalarFieldType.timestamp,
						},
						deviceId: {
							type: ScalarFieldType.string,
						},
					},
				}),
			).toEqual(
				`CREATE EXTERNAL TABLE fooDatabase.barTable (` +
					'`reported` struct<acc:struct<ts:bigint, v:array<float>>, bat:struct<ts:bigint, v:int>, gps:struct<ts:bigint, v:struct<acc:float, alt:float, hdg:float, lat:float, lng:float, spd:float>>>, `timestamp` timestamp, `deviceId` string' +
					') ' +
					"ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' " +
					`WITH SERDEPROPERTIES ('serialization.format' = '1','ignore.malformed.json' = 'true')` +
					` LOCATION 's3://bazBucket/' ` +
					"TBLPROPERTIES ('has_encrypted_data'='false');",
			)
		})
	})
})
