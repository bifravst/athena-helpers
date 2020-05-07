export enum ScalarFieldType {
	timestamp = 'timestamp',
	string = 'string',
	float = 'float',
	int = 'int',
	bigint = 'bigint',
	boolean = 'boolean',
}

export enum StructFieldType {
	struct = 'struct',
}

export enum ArrayFieldType {
	array = 'array',
}

export type ScalarField = {
	type: ScalarFieldType
}

export type StructField = {
	type: StructFieldType
	fields: {
		[key: string]: Field
	}
}

export type ArrayField = {
	type: ArrayFieldType
	items: ScalarFieldType
}

export type Field = ScalarField | StructField | ArrayField

type FieldType = ScalarFieldType | StructFieldType | ArrayFieldType

const createFieldDefinition = ({
	type,
	items,
	fields,
}: {
	type: FieldType
	items?: ScalarFieldType
	fields?: {
		[key: string]: Field
	}
}): string => {
	switch (type) {
		case ScalarFieldType.float:
		case ScalarFieldType.int:
		case ScalarFieldType.bigint:
		case ScalarFieldType.timestamp:
		case ScalarFieldType.string:
		case ScalarFieldType.boolean:
			return type
		case ArrayFieldType.array:
			return `array<${createFieldDefinition({
				type: items as ScalarFieldType,
			})}>`
		case StructFieldType.struct:
			return `struct<${Object.entries(
				fields as {
					[key: string]: Field
				},
			)
				.map(
					([field, definition]) =>
						`${field}:${createFieldDefinition(definition)}`,
				)
				.join(', ')}>`
		default:
			throw new Error(`Unknown field definition: ${type}!`)
	}
}

/**
 * Returns the SQL to create the Athena table
 * @param database Name of the Athena database
 * @param table Name of the Table
 * @param s3Location Name of the S3 bucket that contains the device messages
 * @param fields The list of fields that describe the device data
 */
export const createTableSQL = ({
	database,
	table,
	s3Location,
	fields,
}: {
	database: string
	table: string
	s3Location: string
	fields: {
		[key: string]: Field
	}
}): string => {
	return (
		`CREATE EXTERNAL TABLE ${database}.${table} (` +
		Object.entries(fields)
			.map(([name, field]) => `\`${name}\` ${createFieldDefinition(field)}`)
			.join(', ') +
		') ' +
		"ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' " +
		'WITH SERDEPROPERTIES (' +
		"'serialization.format' = '1'" +
		`) LOCATION '${s3Location}' ` +
		"TBLPROPERTIES ('has_encrypted_data'='false');"
	)
}
