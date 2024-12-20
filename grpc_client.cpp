#include <grpcpp/grpcpp.h>
#include "com/rawlabs/protocol/das/v1/services/registration_service.grpc.pb.h"
#include "com/rawlabs/protocol/das/v1/services/tables_service.grpc.pb.h"
#include "com/rawlabs/protocol/das/v1/services/functions_service.grpc.pb.h"
#include "com/rawlabs/protocol/das/v1/tables/tables.pb.h"
#include "com/rawlabs/protocol/das/v1/common/das.pb.h"
#include "com/rawlabs/protocol/das/v1/types/values.pb.h"
#include "com/rawlabs/protocol/das/v1/types/types.pb.h"
#include "grpc_client.h"

#include <map>

extern "C" {
    #include <postgres.h>

    #include "fmgr.h"
    #include "access/htup_details.h"
    #include "catalog/pg_type.h"
    #include "optimizer/optimizer.h"
    #include "utils/builtins.h"
    #include "utils/date.h"
    #include "utils/datetime.h"
    #include "utils/jsonb.h"
    #include "utils/array.h"
    #include "utils/lsyscache.h"
    #include "utils/syscache.h"
    #include "common/base64.h"
}

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using com::rawlabs::protocol::das::v1::common::DASId;
using com::rawlabs::protocol::das::v1::common::DASDefinition;
using com::rawlabs::protocol::das::v1::query::Operator;
using com::rawlabs::protocol::das::v1::services::RegistrationService;
using com::rawlabs::protocol::das::v1::services::RegisterRequest;
using com::rawlabs::protocol::das::v1::services::UnregisterResponse;
using com::rawlabs::protocol::das::v1::services::OperationsSupportedResponse;
using com::rawlabs::protocol::das::v1::services::GetTableEstimateRequest;
using com::rawlabs::protocol::das::v1::services::TablesService;
using com::rawlabs::protocol::das::v1::services::GetTableDefinitionsRequest;
using com::rawlabs::protocol::das::v1::services::GetTableDefinitionsResponse;
using com::rawlabs::protocol::das::v1::services::GetTableUniqueColumnRequest;
using com::rawlabs::protocol::das::v1::services::GetTableUniqueColumnResponse;
using com::rawlabs::protocol::das::v1::services::InsertTableRequest;
using com::rawlabs::protocol::das::v1::services::InsertTableResponse;
using com::rawlabs::protocol::das::v1::services::UpdateTableRequest;
using com::rawlabs::protocol::das::v1::services::UpdateTableResponse;
using com::rawlabs::protocol::das::v1::services::DeleteTableRequest;
using com::rawlabs::protocol::das::v1::services::DeleteTableResponse;
using com::rawlabs::protocol::das::v1::tables::TableId;
using com::rawlabs::protocol::das::v1::tables::Rows;
using com::rawlabs::protocol::das::v1::tables::Row;
using com::rawlabs::protocol::das::v1::tables::Column;
using com::rawlabs::protocol::das::v1::tables::TableDefinition;
using com::rawlabs::protocol::das::v1::tables::ColumnDefinition;
using com::rawlabs::protocol::das::v1::types::Type;
using com::rawlabs::protocol::das::v1::types::AttrType;
using com::rawlabs::protocol::das::v1::types::RecordType;
using com::rawlabs::protocol::das::v1::types::Value;
using com::rawlabs::protocol::das::v1::types::ValueList;

// #define USECS_PER_HOUR    3600000000L
// #define USECS_PER_MINUTE  60000000L
// #define USECS_PER_SEC     1000000L
#define USECS_PER_MSEC    1000L
#define USECS_PER_MICRO 1L
// #define USECS_PER_DAY     86400000000L

struct TypeInfo {
    std::string sql_type;
    bool nullable;
};

Datum ConvertValueListToArray(const ValueList& value_list, Oid elem_type);

void ValueToJsonbValue(const Value& value, JsonbParseState **pstate, JsonbValue** r);

TypeInfo GetTypeInfo(const Type& my_type) {
    std::string inner_type;
    switch (my_type.type_case()) {
        case Type::kByte:
            return { "smallint", my_type.byte().nullable() };
        case Type::kShort:
            return { "smallint", my_type.short_().nullable() };
        case Type::kInt:
            return { "integer", my_type.int_().nullable() };
        case Type::kLong:
            return { "bigint", my_type.long_().nullable() };
        case Type::kFloat:
            return { "real", my_type.float_().nullable() };
        case Type::kDouble:
            return { "double precision", my_type.double_().nullable() };
        case Type::kDecimal:
            return { "decimal", my_type.decimal().nullable() };
        case Type::kString:
            return { "text", my_type.string().nullable() };
        case Type::kBool:
            return { "boolean", my_type.bool_().nullable() };
        case Type::kBinary:
            return { "bytea", my_type.binary().nullable() };
        case Type::kDate:
            return { "date", my_type.date().nullable() };
        case Type::kTime:
            return { "time", my_type.time().nullable() };
        case Type::kTimestamp:
            return { "timestamp", my_type.timestamp().nullable() };
        case Type::kInterval:
            return { "interval", my_type.interval().nullable() };            
        case Type::kRecord:
            // Make it a JSONB if any field isn't a string
            for (AttrType a : my_type.record().atts()) {
                Type att_type = a.tipe();
                if (att_type.type_case() != Type::kString) {
                    return { "jsonb", my_type.record().nullable() };
                }
            }
            // We want HSTORE in principle, but adding HSTORE support here is still TODO
            return { "jsonb", my_type.record().nullable() };
        case Type::kList:
            inner_type = GetTypeInfo(my_type.list().inner_type()).sql_type;
            return { inner_type + "[]", my_type.list().nullable() };
        case Type::kAny:
            // AnyType does not have a nullable flag
            return { "jsonb", false };
        default:
            elog(ERROR, "Unsupported type: %d", my_type.type_case());
    }
}

std::string TypeToString(const Type& my_type)
{
    return GetTypeInfo(my_type).sql_type;
}

std::string TypeToStringForPushability(const Type& my_type)
{
    Type::TypeCase type_case = my_type.type_case();
    switch (type_case) {
        case Type::kAny:
            return "\"any\"";
        default:
            return TypeToString(my_type);
    }
}

std::string TypeToOperator(Operator type)
{
    switch (type) {
        case Operator::EQUALS:
            return "=";
        case Operator::NOT_EQUALS:
            return "<>";
        case Operator::LESS_THAN:
            return "<";
        case Operator::LESS_THAN_OR_EQUAL:
            return "<=";
        case Operator::GREATER_THAN:
            return ">";
        case Operator::GREATER_THAN_OR_EQUAL:
            return ">=";
        case Operator::LIKE:
            return "~~";
        case Operator::NOT_LIKE:
            return "!~~";
        case Operator::PLUS:
            return "+";
        case Operator::MINUS:
            return "-";
        case Operator::TIMES:
            return "*";
        case Operator::DIV:
            return "/";
        case Operator::MOD:
            return "%";
        default:
            elog(ERROR, "Unsupported operator type: %d", type);
    }
}

char* TableDefinitionToCreateTableSQL(const TableDefinition& definition, const char* das_id, const char* server_name)
{
    std::string sql = "CREATE FOREIGN TABLE " + definition.table_id().name() + " (";
    for (int i = 0; i < definition.columns_size(); ++i) {
        const ColumnDefinition& column = definition.columns(i);
        TypeInfo type_info = GetTypeInfo(column.type());
        sql += column.name() + " " + type_info.sql_type;
        if (type_info.nullable) {
            sql += " NULL";
        } else {
            sql += " NOT NULL";
        }
        if (i < definition.columns_size() - 1) {
            sql += ", ";
        }
    }
    sql += ") SERVER " + std::string(server_name) + " OPTIONS (das_id '" + std::string(das_id) + "')";

    // Duplicate the string in PostgreSQL's memory context
    char* result = (char*) palloc(sql.length() + 1);
    strcpy(result, sql.c_str());

    return result;
}

void ValueToDatum(const Value& value, Oid pgtyp, int32 pgtypmod, Datum* datum, bool* null)
{
    elog(WARNING, "ValueToDatum: %d %d", value.value_case(), pgtyp);
    if (value.has_null()) {
        *null = true;
        *datum = PointerGetDatum(NULL);
    } else {
        *null = false;
        if (pgtyp == INT2OID) {
            // byte or short
            if (value.has_byte()) {
                *datum = Int16GetDatum(value.byte().v());
            } else if (value.has_short_()) {
                *datum = Int16GetDatum(value.short_().v());
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == INT4OID) {
            if (value.has_int_()) {
                *datum = Int32GetDatum(value.int_().v());
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == INT8OID) {
            if (value.has_long_()) {
                *datum = Int64GetDatum(value.long_().v());
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == FLOAT4OID) {
            if (value.has_float_()) {
                *datum = Float4GetDatum(value.float_().v());
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == FLOAT8OID) {
            if (value.has_double_()) {
                *datum = Float8GetDatum(value.double_().v());
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == NUMERICOID) {
            if (value.has_decimal()) {
                *datum = DirectFunctionCall1(numeric_in, CStringGetDatum(value.decimal().v().c_str()));
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == TEXTOID) {
            if (value.has_string()) {
                char *dup_str = pstrdup(value.string().v().c_str());
                text* txt = cstring_to_text(dup_str);
                *datum = PointerGetDatum(txt);
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == BOOLOID) {
            if (value.has_bool_()) {
                *datum = BoolGetDatum(value.bool_().v());
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == BYTEAOID) {
            if (value.has_binary()) {
                const char *binary_data = value.binary().v().c_str();
                size_t value_size = value.binary().v().size();
                bytea *bin_data = (bytea *) palloc(value_size + VARHDRSZ);
                SET_VARSIZE(bin_data, value_size + VARHDRSZ);
                memcpy(VARDATA(bin_data), binary_data, value_size);
                *datum = PointerGetDatum(bin_data);
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == DATEOID) {
            if (value.has_date()) {
                auto date_obj = value.date();
                int32_t year = date_obj.year();
                int32_t month = date_obj.month();
                int32_t day = date_obj.day();

                DateADT date = date2j(year, month, day) - POSTGRES_EPOCH_JDATE;

                *datum = Int32GetDatum(date);
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == TIMEOID) {
            if (value.has_time()) {

                auto time_obj = value.time();

                int32_t hour = time_obj.hour();
                int32_t minute = time_obj.minute();
                int32_t second = time_obj.second();
                int32_t nano = time_obj.nano();

                int64_t microseconds = ((int64_t) hour * USECS_PER_HOUR) +
                                        ((int64_t) minute * USECS_PER_MINUTE) +
                                        ((int64_t) second * USECS_PER_SEC) +
                                        ((int64_t) nano / 1000);

                TimeADT time_value = (TimeADT) microseconds;

                *datum = Int64GetDatum(time_value);
            } else {
                elog(ERROR, "unsupported");
            }
        } else if (pgtyp == TIMESTAMPOID) {
            if (value.has_timestamp()) {
                auto timestamp_obj = value.timestamp();

                int32_t year = timestamp_obj.year();
                int32_t month = timestamp_obj.month();
                int32_t day = timestamp_obj.day();

                int32_t hour = timestamp_obj.hour();
                int32_t minute = timestamp_obj.minute();
                int32_t second = timestamp_obj.second();
                int32_t nano = timestamp_obj.nano();

                DateADT date = date2j(year, month, day) - POSTGRES_EPOCH_JDATE;

                int64_t microseconds = ((int64_t) hour * USECS_PER_HOUR) +
                                        ((int64_t) minute * USECS_PER_MINUTE) +
                                        ((int64_t) second * USECS_PER_SEC) +
                                        ((int64_t) nano / 1000);

                // Assign to TimestampTz (microseconds since epoch)
                // Note: This simplistic conversion assumes date2j returns days since a fixed epoch
                // Adjust the calculation based on the actual definition of date2j and TimestampTz
                TimestampTz timestamp_value = ((TimestampTz) date * USECS_PER_DAY) + microseconds;

                *datum = TimestampTzGetDatum(timestamp_value);
            }
        } else if (pgtyp == INTERVALOID) {
            if (value.has_interval()) {
                auto interval_obj = value.interval();

                Interval* v = (Interval*) palloc(sizeof(Interval));

                v->time = ((int64_t) interval_obj.hours() * USECS_PER_HOUR) +
                        ((int64_t) interval_obj.minutes() * USECS_PER_MINUTE) +
                        ((int64_t) interval_obj.seconds() * USECS_PER_SEC) +
                        ((int64_t) interval_obj.micros() * USECS_PER_MICRO);

                v->day = interval_obj.days();
                v->month = interval_obj.months();
                elog(ERROR, "This is broken; does not handle years?");
                // v->year = interval_obj.years();

                *datum = PointerGetDatum(v);
            }
        } else if (pgtyp == JSONBOID) {
            JsonbParseState *state = NULL;
            JsonbValue *res = NULL;
            ValueToJsonbValue(value, &state, &res);
            Jsonb *jsonb = JsonbValueToJsonb(res);
            *datum = JsonbPGetDatum(jsonb);
        } else if (pgtyp == BOOLARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), BOOLOID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == INT2ARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), INT2OID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == INT4ARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), INT4OID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == INT8ARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), INT8OID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == FLOAT4ARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), FLOAT4OID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == FLOAT8ARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), FLOAT8OID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == NUMERICARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), NUMERICOID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == TEXTARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), TEXTOID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == DATEARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), DATEOID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == TIMEARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), TIMEOID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == TIMESTAMPARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), TIMESTAMPOID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == INTERVALARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), INTERVALOID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else if (pgtyp == JSONBARRAYOID) {
            if (value.has_list()) {
                *datum = ConvertValueListToArray(value.list(), JSONBOID);
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
        } else {
            elog(ERROR, "Unsupported value type: %d", value.value_case());
        }
    }
}

Datum ConvertValueListToArray(const ValueList& value_list, Oid elem_type)
{
    int nelems = value_list.values_size();

    // Default to text[] for empty arrays
    if (nelems == 0) {
        ArrayType *empty_array = construct_empty_array(elem_type);
        return PointerGetDatum(empty_array);
    }

    int16 elem_len;
    bool elem_byval;
    char elem_align;

    // Get type information
    get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

    // Allocate memory for Datum array and null flags
    Datum *elems = (Datum *) palloc(sizeof(Datum) * nelems);
    bool *nulls = (bool *) palloc(sizeof(bool) * nelems);

    // Convert each Value to Datum
    for (int i = 0; i < nelems; ++i) {
        const Value& val = value_list.values(i);
        Datum elem_datum;
        bool is_null;

        // Use ValueToDatum to convert each Value to Datum
        ValueToDatum(val, elem_type, -1, &elem_datum, &is_null);

        elems[i] = elem_datum;
        nulls[i] = is_null;
    }

    // Build the array
    ArrayType *result_array = construct_array(elems, nelems, elem_type, elem_len, elem_byval, elem_align);

    // Free temporary memory
    pfree(elems);
    pfree(nulls);

    return PointerGetDatum(result_array);
}

void ValueToJsonbValue(const Value& value, JsonbParseState **pstate, JsonbValue** r)
{
    if (value.has_record()) {
        pushJsonbValue(pstate, WJB_BEGIN_OBJECT, NULL);
        for (const auto& att : value.record().atts()) {
            JsonbValue key;
            key.type = jbvString;
            key.val.string.val = pstrdup(att.name().c_str());
            key.val.string.len = att.name().length();
            pushJsonbValue(pstate, WJB_KEY, &key);
            JsonbValue* att_value = NULL;
            ValueToJsonbValue(att.value(), pstate, &att_value);
            pushJsonbValue(pstate, WJB_VALUE, att_value);
        }
        *r = pushJsonbValue(pstate, WJB_END_OBJECT, NULL);
    } else if (value.has_list()) {
        pushJsonbValue(pstate, WJB_BEGIN_ARRAY, NULL);
        for (const auto& item : value.list().values()) {
            JsonbValue* item_value = NULL;
            ValueToJsonbValue(item, pstate, &item_value);
            pushJsonbValue(pstate, WJB_VALUE, item_value);
        }
        *r = pushJsonbValue(pstate, WJB_END_ARRAY, NULL);
    } else {
        JsonbValue* v = (JsonbValue*)palloc(sizeof(JsonbValue));
        if (value.has_null()) {
            v->type = jbvNull;
        } else if (value.has_string()) {
            v->type = jbvString;
            std::string str = value.string().v();
            v->val.string.val = pstrdup(str.c_str());
            v->val.string.len = str.length();
        } else if (value.has_bool_()) {
            v->type = jbvBool;
            v->val.boolean = value.bool_().v();
        } else if (value.has_binary()) {
            // Represent binary data as base64 string using pg_b64_encode
            std::string binary_data = value.binary().v();
            int binary_len = binary_data.length();

            // Calculate the length required for the base64 encoded data
            size_t encoded_len = pg_b64_enc_len(binary_len);

            // Allocate memory for the encoded data
            char *encoded = (char *) palloc(encoded_len + 1); // +1 for null terminator

            // Encode the binary data
            int actual_encoded_len = pg_b64_encode((const char *) binary_data.c_str(), binary_len, encoded, encoded_len);

            if (actual_encoded_len < 0) {
                elog(ERROR, "Error encoding binary data to base64");
            }

            // Null-terminate the encoded string
            encoded[actual_encoded_len] = '\0';

            v->type = jbvString;
            v->val.string.val = encoded;
            v->val.string.len = actual_encoded_len;
        } else if (value.has_date()) {
            auto date_obj = value.date();
            char buf[11]; // YYYY-MM-DD\0
            snprintf(buf, sizeof(buf), "%04d-%02d-%02d", date_obj.year(), date_obj.month(), date_obj.day());
            v->type = jbvString;
            v->val.string.val = pstrdup(buf);
            v->val.string.len = strlen(buf);
        } else if (value.has_time()) {
            auto time_obj = value.time();
            char buf[13]; // HH:MM:SS\0
            snprintf(buf, sizeof(buf), "%02d:%02d:%02d", time_obj.hour(), time_obj.minute(), time_obj.second());
            v->type = jbvString;
            v->val.string.val = pstrdup(buf);
            v->val.string.len = strlen(buf);
        } else if (value.has_timestamp()) {
            auto timestamp_obj = value.timestamp();
            char buf[30]; // YYYY-MM-DDTHH:MM:SS.ssssssZ\0
            snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ",
                     timestamp_obj.year(), timestamp_obj.month(), timestamp_obj.day(),
                     timestamp_obj.hour(), timestamp_obj.minute(), timestamp_obj.second(),
                     timestamp_obj.nano() / 1000);
            v->type = jbvString;
            v->val.string.val = pstrdup(buf);
            v->val.string.len = strlen(buf);
        } else if (value.has_interval()) {
            auto interval_obj = value.interval();
            std::ostringstream oss;
            oss << "P";
            if (interval_obj.years() != 0) oss << interval_obj.years() << "Y";
            if (interval_obj.months() != 0) oss << interval_obj.months() << "M";
            if (interval_obj.days() != 0) oss << interval_obj.days() << "D";
            if (interval_obj.hours() != 0 || interval_obj.minutes() != 0 || interval_obj.seconds() != 0 || interval_obj.micros() != 0) {
                oss << "T";
                if (interval_obj.hours() != 0) oss << interval_obj.hours() << "H";
                if (interval_obj.minutes() != 0) oss << interval_obj.minutes() << "M";
                if (interval_obj.seconds() != 0 || interval_obj.micros() != 0) {
                    double seconds = interval_obj.seconds() + interval_obj.micros() / 1000000.0;
                    oss << seconds << "S";
                }
                elog(ERROR, "Broken; where are the micros?");
            }
            std::string str = oss.str();
            v->type = jbvString;
            v->val.string.val = pstrdup(str.c_str());
            v->val.string.len = str.length();
        } else {
            // number
            Datum numDatum;
            if (value.has_byte()) {
                numDatum = DirectFunctionCall1(int2_numeric, Int16GetDatum(value.byte().v()));
            } else if (value.has_short_()) {
                numDatum = DirectFunctionCall1(int2_numeric, Int16GetDatum(value.short_().v()));
            } else if (value.has_int_()) {
                numDatum = DirectFunctionCall1(int4_numeric, Int32GetDatum(value.int_().v()));
            } else if (value.has_long_()) {
                numDatum = DirectFunctionCall1(int8_numeric, Int64GetDatum(value.long_().v()));
            } else if (value.has_float_()) {
                numDatum = DirectFunctionCall1(float4_numeric, Float4GetDatum(value.float_().v()));
            } else if (value.has_double_()) {
                numDatum = DirectFunctionCall1(float8_numeric, Float8GetDatum(value.double_().v()));
            } else if (value.has_decimal()) {
                numDatum = DirectFunctionCall1(numeric_in, CStringGetDatum(value.decimal().v().c_str()));
            } else {
                elog(ERROR, "Unsupported value type: %d", value.value_case());
            }
            Numeric num = DatumGetNumeric(numDatum);
            v->type = jbvNumeric;
            v->val.numeric = num;
        }
        *r = v;
    }
}

Value DatumToValue(Datum datum, Oid pgtyp, int32 pgtypmod)
{
    Value value;
    switch (pgtyp) {
        case INT4OID:
            value.mutable_int_()->set_v(DatumGetInt32(datum));
            break;
        case INT8OID:
            value.mutable_long_()->set_v(DatumGetInt64(datum));
            break;
        case FLOAT4OID:
            value.mutable_float_()->set_v(DatumGetFloat4(datum));
            break;
        case FLOAT8OID:
            value.mutable_double_()->set_v(DatumGetFloat8(datum));
            break;
        case TEXTOID:
            {
                text* txt = DatumGetTextP(datum);
                value.mutable_string()->set_v(std::string(VARDATA(txt), VARSIZE(txt) - VARHDRSZ));
            }
            break;
        case BOOLOID:
            value.mutable_bool_()->set_v(DatumGetBool(datum));
            break;
        default:
            elog(ERROR, "Unsupported pgtype: %d", pgtyp);
    }
    return value;
}


struct SqlQueryIterator
{
    // std::unique_ptr<QueryService::Stub> stub;
    grpc::ClientContext context;
    std::unique_ptr<grpc::ClientReader<Rows>> reader;
    Rows current_rows;            // Buffer for current Rows message
    int current_row_index;        // Index within current_rows
    das_opt* opts;
    char* sql;
    bool started;
};

// Expose the functions to C
extern "C" {

void wait_for_server(das_opt* opts)
{

}

//////////////////////////////////////////////////////////////////////////////////////////
// Registration Service
//////////////////////////////////////////////////////////////////////////////////////////

char* register_das(das_opt* opts)
{
    ListCell *lc_key;
    ListCell *lc_value;

    elog(DEBUG3, "Registering DAS with URL: %s, type: %s, id: %s", opts->das_url, opts->das_type, opts->das_id);

    auto client = RegistrationService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));
    std::map<std::string, std::string> options;
    forboth(lc_key, opts->option_keys, lc_value, opts->option_values)
    {
        std::string key((const char *) lfirst(lc_key));
        std::string value((const char *) lfirst(lc_value));
        options[key] = value;
    }

    RegisterRequest request;
    DASDefinition* definition = request.mutable_definition();

    definition->set_type(std::string(opts->das_type));

    for (const auto& [key, value] : options)
        (*definition->mutable_options())[key] = value;

    std::string das_id_str(opts->das_id ? opts->das_id : "");
    if (!das_id_str.empty()) {
        request.mutable_id()->set_id(das_id_str);
    }

    DASId response;
    ClientContext context;
    Status status = client->Register(&context, request, &response);

    if (!status.ok())
        elog(ERROR, "Failed to register DAS: %s", status.error_message().c_str());

    // Duplicate the string in PostgreSQL's memory context
    return pstrdup(response.id().c_str());
}

void unregister_das(das_opt* opts)
{
    elog(DEBUG3, "Unregistering DAS with URL: %s, id: %s", opts->das_url, opts->das_id);

    auto client = RegistrationService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    DASId request;
    request.set_id(std::string(opts->das_id));

    UnregisterResponse response;
    ClientContext context;
    Status status = client->Unregister(&context, request, &response);

    if (!status.ok())
        elog(ERROR, "Failed to unregister DAS: %s", status.error_message().c_str());
}

char** get_operations_supported(das_opt* opts, bool* orderby_supported, bool* join_supported, bool* aggregation_supported, int* pushability_len)
{
    elog(DEBUG3, "Getting supported operations for DAS with URL: %s, id: %s", opts->das_url, opts->das_id);

    auto client = RegistrationService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    DASId request;
    request.set_id(std::string(opts->das_id));

    OperationsSupportedResponse response;
    grpc::ClientContext context;
    grpc::Status status = client->OperationsSupported(&context, request, &response);

    if (!status.ok()) {
        // Wait for the server to be available
        if (status.error_code() == grpc::StatusCode::UNAVAILABLE)
            wait_for_server(opts);

        // Retry if the DAS is not found
        if (opts->das_id && status.error_message().find("DAS not found") != std::string::npos) {
            register_das(opts);
            return get_operations_supported(opts, orderby_supported, join_supported, aggregation_supported, pushability_len);
        }

        elog(ERROR, "Failed to get supported operations: %s", status.error_message().c_str());
    }

    *orderby_supported = true; // response.orderbysupported();
    *join_supported = true; // response.joinsupported();
    *aggregation_supported = true; // response.aggregationsupported();
    elog(DEBUG3, "Order by: %d, Join: %d, Aggregation: %d", *orderby_supported, *join_supported, *aggregation_supported);

    std::vector<std::string> operations_supported;

    // Process the functions supported
    for (const auto& function : response.functionssupported())
    {
        std::string function_str = "ROUTINE pg_catalog." + function.name() + "(";
        bool first = true;
        for (const auto& param : function.parameters())
        {
            if (!first) 
            {
                function_str += ",";
            }
            function_str += TypeToStringForPushability(param);
            first = false;
        }
        function_str += ")";
        operations_supported.push_back(function_str);

        // elog(WARNING, "Function: %s", function_str.c_str());
    }

    // Process the operators supported
    for (const auto& op : response.operatorssupported()) {
        std::string operator_str = "OPERATOR pg_catalog." + TypeToOperator(op.operator_())
            + "(" + TypeToString(op.lhs()) + "," + TypeToString(op.rhs()) + ")";
        operations_supported.push_back(operator_str);

        // elog(WARNING, "Operator: %s", operator_str.c_str());
    }

    // Copy the strings to PostgreSQL's memory context
    *pushability_len = operations_supported.size();
    char** result = (char**) palloc(sizeof(char*) * (*pushability_len));
    for (int i = 0; i < *pushability_len; ++i)
    {
        result[i] = (char*) palloc(strlen(operations_supported[i].c_str()) + 1);
        strcpy(result[i], operations_supported[i].c_str());
    }
    return result;
}

void free_operations_supported(char** pushability_list, int pushability_len)
{
    elog(DEBUG3, "Freeing supported operations");

    for (int i = 0; i < pushability_len; ++i)
        pfree(pushability_list[i]);
    pfree(pushability_list);
}

//////////////////////////////////////////////////////////////////////////////////////////
// Table Definitions Service
//////////////////////////////////////////////////////////////////////////////////////////

char** get_table_definitions(das_opt* opts, const char* server_name, int* num_tables)
{
    elog(DEBUG3, "Getting table definitions for DAS with URL: %s, id: %s, server: %s", opts->das_url, opts->das_id, server_name);

    auto client = TablesService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    GetTableDefinitionsRequest request;
    request.mutable_das_id()->set_id(opts->das_id);

    GetTableDefinitionsResponse response;
    ClientContext context;
    Status status = client->GetTableDefinitions(&context, request, &response);
    if (!status.ok())
        elog(ERROR, "GetTableDefinitions RPC failed: %s", status.error_message().c_str());
    
    // Copy the table definitions to PostgreSQL's memory context
    *num_tables = response.definitions_size();
    char** result = (char**) palloc(sizeof(char*) * (*num_tables));
    for (int i = 0; i < *num_tables; ++i)
    {
        result[i] = TableDefinitionToCreateTableSQL(response.definitions(i), opts->das_id, server_name);
    }
    return result;
}

//////////////////////////////////////////////////////////////////////////////////////////
// SQL Service
//////////////////////////////////////////////////////////////////////////////////////////

void get_query_estimate(das_opt* opts, const char* sql, double* rows, double* width)
{
    // elog(ERROR, "TO DO");
    *rows = 1000;
    *width = 200;
    

    // elog(WARNING, "Getting query estimate for DAS with URL: %s, id: %s, SQL: %s", opts->das_url, opts->das_id, sql);

    // auto client = QueryService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    // QueryRequest request;
    // DASId* dasId = request.mutable_das_id();
    // dasId->set_id(opts->das_id);
    // request.set_sql(sql);

    // GetQueryEstimateResponse response;
    // ClientContext context;
    // Status status = client->GetQueryEstimate(&context, request, &response);

    // if (!status.ok())
    //     elog(ERROR, "Failed to get query estimate: %s", status.error_message().c_str());

    // *rows = response.rows();
    // *width = response.bytes();

    elog(WARNING, "Got query estimate: rows: %f, width: %f", *rows, *width);
}

SqlQueryIterator* sql_query_iterator_init(das_opt* opts, const char* sql, const char* plan_id)
{
    elog(ERROR, "TO DO");

    // elog(WARNING, "Initializing SQL query iterator for DAS with URL: %s, das_id: %s, plan_id: %s, SQL: %s", opts->das_url, opts->das_id, plan_id, sql);

    // void *mem = palloc(sizeof(SqlQueryIterator));
    // auto iterator = new (mem) SqlQueryIterator();
    // iterator->stub = QueryService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));
    // iterator->opts = opts;
    // iterator->sql = pstrdup(sql);
    // iterator->started = false;
    // QueryRequest request;
    // DASId* dasId = request.mutable_das_id();
    // dasId->set_id(opts->das_id);
    // request.set_sql(sql);
    // request.set_planid(plan_id);
    // iterator->reader = iterator->stub->ExecuteQuery(&iterator->context, request);
    // iterator->current_row_index = 0;
    // return iterator;
}

bool sql_query_iterator_next(SqlQueryIterator* iterator, int* attnums, Datum* dvalues, bool* nulls, Oid* pgtypes, int32* pgtypmods)
{
    elog(ERROR, "TO DO");

    // elog(WARNING, "Fetching next row");

    // // Check if we need to read another chunk of rows
    // if (iterator->current_row_index >= iterator->current_rows.rows_size())
    // {
    //     if (!iterator->reader->Read(&iterator->current_rows))
    //     {
    //         grpc::Status status = iterator->reader->Finish();
        
    //         if (!status.ok()) {
    //             if (!iterator->started)
    //             {
    //                 // Wait for the server to be available
    //                 if (status.error_code() == grpc::StatusCode::UNAVAILABLE)
    //                     wait_for_server(iterator->opts);                
    //                 elog(WARNING, "bad status1");
    //                 // Retry if the DAS is not found
    //                 if (iterator->opts->das_id && status.error_message().find("DAS not found") != std::string::npos) {
    //                     elog(WARNING, "bad status2");
    //                     register_das(iterator->opts);
    //                     elog(WARNING, "bad status3");
    //                     QueryRequest request;
    //                     DASId* dasId = request.mutable_das_id();
    //                     dasId->set_id(iterator->opts->das_id);
    //                     request.set_sql(iterator->sql);
    //                     elog(WARNING, "bad status4 %s das_id %s", iterator->opts->das_url, iterator->opts->das_id);
    //                     iterator->stub = QueryService::NewStub(grpc::CreateChannel(iterator->opts->das_url, grpc::InsecureChannelCredentials()));
    //                     elog(WARNING, "bad status5");
                        
    //                     iterator->context.~ClientContext();  // Destroy the old context
    //                     new (&iterator->context) grpc::ClientContext();  // Reconstruct a new context in-place

    //                     iterator->reader = iterator->stub->ExecuteQuery(&iterator->context, request);
    //                     elog(WARNING, "bad status6");
    //                     return sql_query_iterator_next(iterator, attnums, dvalues, nulls, pgtypes, pgtypmods);
    //                 }
    //             }

    //             elog(ERROR, "gRPC stream failed with error: %s, code: %d", status.error_message().c_str(), status.error_code());
    //         }

    //         elog(DEBUG1, "No more data");
    //         return false;
    //     }
    //     iterator->started = true;
    //     iterator->current_row_index = 0;

    //     if (iterator->current_rows.rows_size() == 0)
    //     {
    //         elog(WARNING, "Fetched next Rows message with 0 rows");
    //         return false;
    //     }
    // }

    // // Get the current row
    // const Row& row = iterator->current_rows.rows(iterator->current_row_index++);
    // const auto& columns = row.columns();

    // int num_columns = columns.size();
    // for (int i = 0; i < num_columns; ++i)
    // {
    //     const Column& column = columns[i];
    //     const Value& value = column.data();
    //     int attnum = attnums[i];

    //     elog(DEBUG3, "Processing field %d (attnum %d) with \"%s\"", i, attnum, column.name().c_str());

    //     ValueToDatum(value, pgtypes[i], pgtypmods[i], &dvalues[attnum], &nulls[attnum]);
    // }

    // return true;
}

void sql_query_iterator_close(SqlQueryIterator* iterator)
{
    elog(ERROR, "TO DO");
    
    // elog(DEBUG3, "Closing SQL query iterator");

    // Rows response;

    // // // TODO (msb): This is a hack to drain the iterator. It is VERY expensive.
    // // while (iterator->reader->Read(&response))
    // //     // Optionally process the response or discard it
    // //     ;

    // // grpc::Status status = iterator->reader->Finish();
    // // if (!status.ok())
    // //     elog(ERROR, "gRPC error: %s", status.error_message().c_str());

    // iterator->~SqlQueryIterator();
    // pfree(iterator);
}

//////////////////////////////////////////////////////////////////////////////////////////
// Table Update Service
//////////////////////////////////////////////////////////////////////////////////////////

char* unique_column(das_opt* opts, const char* table_name)
{
    elog(DEBUG3, "Getting unique column for DAS with URL: %s, id: %s, table: %s", opts->das_url, opts->das_id, table_name);

    auto client = TablesService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    GetTableUniqueColumnRequest request;

    DASId* dasId = request.mutable_das_id();
    dasId->set_id(opts->das_id);

    TableId* tableId = request.mutable_table_id();
    tableId->set_name(table_name);

    GetTableUniqueColumnResponse response;
    ClientContext context;

    Status status = client->GetTableUniqueColumn(&context, request, &response);

    if (!status.ok()) {
        // Wait for the server to be available
        if (status.error_code() == grpc::StatusCode::UNAVAILABLE)
            wait_for_server(opts);

        // Retry if the DAS is not found
        if (opts->das_id && status.error_message().find("DAS not found") != std::string::npos) {
            register_das(opts);
            return unique_column(opts, table_name);
        }

        elog(ERROR, "Failed to get unique column: %s", status.error_message().c_str());
    }

    // Duplicate the string in PostgreSQL's memory context
    return pstrdup(response.column().c_str());
}

void insert_row(das_opt* opts, const char* table_name, int num_columns, int* attnums, char **attnames, Datum* dvalues, bool* nulls, Oid* pgtypes, int32* pgtypmods)
{
    elog(DEBUG3, "Inserting row into table %s for DAS with URL: %s, id: %s", table_name, opts->das_url, opts->das_id);

    auto client = TablesService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    Row row;
    for (int i = 0; i < num_columns; ++i)
    {
        Column* column = row.add_columns();
        column->set_name(std::string(attnames[i]));
        Value* value = column->mutable_data();

        if (nulls[i])
        {
            elog(WARNING, "Inserting NULL value for column slot %s", attnames[i]);
            value->mutable_null();
        }
        else
        {
            elog(WARNING, "Inserting value for column slot %s", attnames[i]);
            Value datum = DatumToValue(dvalues[i], pgtypes[i], pgtypmods[i]);
            value->CopyFrom(datum);
        }
    }

    elog(WARNING, "Sending insert request");
    InsertTableRequest request;
    request.mutable_das_id()->set_id(opts->das_id);
    elog(WARNING, "Sending insert request 2");
    request.mutable_table_id()->set_name(std::string(table_name));
    elog(WARNING, "Sending insert request 3");
    request.mutable_values()->CopyFrom(row);
    elog(WARNING, "Sending insert request 4");

    InsertTableResponse response;
    ClientContext context;
    elog(WARNING, "Sending insert request 5");
    Status status = client->InsertTable(&context, request, &response);
    elog(WARNING, "Sending insert request 6");

    if (!status.ok()) {
        // Wait for the server to be available
        if (status.error_code() == grpc::StatusCode::UNAVAILABLE)
            wait_for_server(opts);

        // Retry if the DAS is not found
        if (opts->das_id && status.error_message().find("DAS not found") != std::string::npos) {
            register_das(opts);
            return insert_row(opts, table_name, num_columns, attnums, attnames, dvalues, nulls, pgtypes, pgtypmods);
        }

        elog(ERROR, "Failed to insert row: %s", status.error_message().c_str());
    }

    elog(DEBUG3, "Row inserted successfully");
}

void update_row(das_opt* opts, const char* table_name, Datum k_value, Oid k_pgtype, int32 k_pgtypmods, int num_columns, int* attnums, char **attnames, Datum* dvalues, bool* nulls, Oid* pgtypes, int32* pgtypmods)
{
    elog(DEBUG3, "Updating row in table %s for DAS with URL: %s, id: %s", table_name, opts->das_url, opts->das_id);

    auto client = TablesService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    Value key_datum = DatumToValue(k_value, k_pgtype, k_pgtypmods);

    Row row;
    for (int i = 0; i < num_columns; ++i)
    {
        Column* column = row.add_columns();
        column->set_name(std::string(attnames[i]));
        Value* value = column->mutable_data();

        if (nulls[i])
        {
            elog(WARNING, "Updating NULL value for column %s", attnames[i]);
            value->mutable_null();
        }
        else
        {
            elog(WARNING, "Updating value for column %s", attnames[i]);
            Value datum = DatumToValue(dvalues[i], pgtypes[i], pgtypmods[i]);
            value->CopyFrom(datum);
        }
    }

    UpdateTableRequest request;
    request.mutable_das_id()->set_id(opts->das_id);
    request.mutable_table_id()->set_name(std::string(table_name));
    request.mutable_row_id()->CopyFrom(key_datum);
    request.mutable_new_values()->CopyFrom(row);

    UpdateTableResponse response;
    ClientContext context;
    Status status = client->UpdateTable(&context, request, &response);

    if (!status.ok()) {
        // Wait for the server to be available
        if (status.error_code() == grpc::StatusCode::UNAVAILABLE)
            wait_for_server(opts);

        // Retry if the DAS is not found
        if (opts->das_id && status.error_message().find("DAS not found") != std::string::npos) {
            register_das(opts);
            return update_row(opts, table_name, k_value, k_pgtype, k_pgtypmods, num_columns, attnums, attnames, dvalues, nulls, pgtypes, pgtypmods);
        }
        
        elog(ERROR, "Failed to update row: %s", status.error_message().c_str());
    }

    elog(DEBUG3, "Row updated successfully");
}

void delete_row(das_opt* opts, const char* table_name, Datum k_value, Oid k_pgtype, int32 k_pgtypmods)
{
    elog(DEBUG3, "Deleting row from table %s for DAS with URL: %s, id: %s", table_name, opts->das_url, opts->das_id);

    auto client = TablesService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    Value key_datum = DatumToValue(k_value, k_pgtype, k_pgtypmods);

    DeleteTableRequest request;
    request.mutable_das_id()->set_id(opts->das_id);
    request.mutable_table_id()->set_name(std::string(table_name));
    request.mutable_row_id()->CopyFrom(key_datum);

    DeleteTableResponse response;
    ClientContext context;
    Status status = client->DeleteTable(&context, request, &response);

    if (!status.ok()) {
        // Wait for the server to be available
        if (status.error_code() == grpc::StatusCode::UNAVAILABLE)
            wait_for_server(opts);

        // Retry if the DAS is not found
        if (opts->das_id && status.error_message().find("DAS not found") != std::string::npos) {
            register_das(opts);
            return delete_row(opts, table_name, k_value, k_pgtype, k_pgtypmods);
        }

        elog(ERROR, "Failed to delete row: %s", status.error_message().c_str());
    }

    elog(DEBUG3, "Row deleted successfully");
}

} // extern "C"
