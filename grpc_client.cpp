#include <grpcpp/grpcpp.h>
#include "com/rawlabs/protocol/das/services/registration_service.grpc.pb.h"
#include "com/rawlabs/protocol/das/services/tables_service.grpc.pb.h"
#include "com/rawlabs/protocol/das/services/query_service.grpc.pb.h"
#include "com/rawlabs/protocol/das/tables.pb.h"
#include "com/rawlabs/protocol/das/rows.pb.h"
#include "com/rawlabs/protocol/das/das.pb.h"
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
using com::rawlabs::protocol::raw::Type;
using com::rawlabs::protocol::raw::Value;
using com::rawlabs::protocol::raw::ValueList;
using com::rawlabs::protocol::das::services::RegistrationService;
using com::rawlabs::protocol::das::services::RegisterRequest;
using com::rawlabs::protocol::das::services::UnregisterResponse;
using com::rawlabs::protocol::das::services::OperationsSupportedResponse;
using com::rawlabs::protocol::das::services::QueryService;
using com::rawlabs::protocol::das::services::QueryRequest;
using com::rawlabs::protocol::das::services::GetQueryEstimateResponse;
using com::rawlabs::protocol::das::services::TablesService;
using com::rawlabs::protocol::das::services::GetTableDefinitionsRequest;
using com::rawlabs::protocol::das::services::GetTableDefinitionsResponse;
using com::rawlabs::protocol::das::services::UniqueColumnRequest;
using com::rawlabs::protocol::das::services::UniqueColumnResponse;
using com::rawlabs::protocol::das::services::InsertRequest;
using com::rawlabs::protocol::das::services::InsertResponse;
using com::rawlabs::protocol::das::services::UpdateRequest;
using com::rawlabs::protocol::das::services::UpdateResponse;
using com::rawlabs::protocol::das::services::DeleteRequest;
using com::rawlabs::protocol::das::services::DeleteResponse;
using com::rawlabs::protocol::raw::Value;
using com::rawlabs::protocol::das::Rows;
using com::rawlabs::protocol::das::Row;
using com::rawlabs::protocol::das::Operator;
using com::rawlabs::protocol::das::OperatorType;
using com::rawlabs::protocol::das::TableId;
using com::rawlabs::protocol::das::DASId;
using com::rawlabs::protocol::das::DASDefinition;
using com::rawlabs::protocol::das::Column;
using com::rawlabs::protocol::das::TableDefinition;
using com::rawlabs::protocol::das::ColumnDefinition;

// #define USECS_PER_HOUR    3600000000L
// #define USECS_PER_MINUTE  60000000L
// #define USECS_PER_SEC     1000000L
#define USECS_PER_MSEC    1000L
// #define USECS_PER_DAY     86400000000L

struct TypeInfo {
    std::string sql_type;
    bool nullable;
};

Datum ConvertValueListToArray(const ValueList& value_list);

JsonbValue* ValueToJsonbValue(const Value& value, JsonbParseState **pstate);

TypeInfo GetTypeInfo(const Type& my_type) {
    std::string inner_type;
    switch (my_type.type_case()) {
        case Type::kUndefined:
            if (my_type.undefined().nullable())
                return { "text", true };
            return { "unknown", false };
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
            return { "jsonb", my_type.record().nullable() };
        case Type::kList:
            inner_type = GetTypeInfo(my_type.list().innertype()).sql_type;
            return { inner_type + "[]", my_type.list().nullable() };
        case Type::kAny:
            // AnyType does not have a nullable flag
            return { "any", false };
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

std::string TypeToOperator(OperatorType type)
{
    switch (type) {
        case OperatorType::EQUALS:
            return "=";
        case OperatorType::NOT_EQUALS:
            return "<>";
        case OperatorType::LESS_THAN:
            return "<";
        case OperatorType::LESS_THAN_OR_EQUAL:
            return "<=";
        case OperatorType::GREATER_THAN:
            return ">";
        case OperatorType::GREATER_THAN_OR_EQUAL:
            return ">=";
        case OperatorType::LIKE:
            return "~~";
        case OperatorType::NOT_LIKE:
            return "!~~";
        case OperatorType::PLUS:
            return "+";
        case OperatorType::MINUS:
            return "-";
        case OperatorType::TIMES:
            return "*";
        case OperatorType::DIV:
            return "/";
        case OperatorType::MOD:
            return "%";
        default:
            elog(ERROR, "Unsupported operator type: %d", type);
    }
}

char* TableDefinitionToCreateTableSQL(const TableDefinition& definition, const char* das_id, const char* server_name)
{
    std::string sql = "CREATE FOREIGN TABLE " + definition.tableid().name() + " (";
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
    elog(WARNING, "ValueToDatum: %d", value.value_case());
    if (value.has_null()) {
        *null = true;
        *datum = PointerGetDatum(NULL);
    } else {
        *null = false;
        if (value.has_byte()) {
            *datum = Int16GetDatum(value.byte().v());
        } else if (value.has_short_()) {
            *datum = Int16GetDatum(value.short_().v());
        } else if (value.has_int_()) {
            *datum = Int32GetDatum(value.int_().v());
        } else if (value.has_long_()) {
            *datum = Int64GetDatum(value.long_().v());
        } else if (value.has_float_()) {
            *datum = Float4GetDatum(value.float_().v());
        } else if (value.has_double_()) {
            *datum = Float8GetDatum(value.double_().v());
        } else if (value.has_decimal()) {
            *datum = DirectFunctionCall1(numeric_in, CStringGetDatum(value.decimal().v().c_str()));
        } else if (value.has_string()) {
            char *dup_str = pstrdup(value.string().v().c_str());
            text* txt = cstring_to_text(dup_str);
            *datum = PointerGetDatum(txt);
        } else if (value.has_bool_()) {
            *datum = BoolGetDatum(value.bool_().v());
        } else if (value.has_binary()) {
            *datum = PointerGetDatum(NULL);
        } else if (value.has_date()) {
            auto date_obj = value.date();
            int32_t year = date_obj.year();
            int32_t month = date_obj.month();
            int32_t day = date_obj.day();

            DateADT date = date2j(year, month, day);

            *datum = Int32GetDatum(date);
        } else if (value.has_time()) {

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
        } else if (value.has_timestamp()) {
            auto timestamp_obj = value.timestamp();

            int32_t year = timestamp_obj.year();
            int32_t month = timestamp_obj.month();
            int32_t day = timestamp_obj.day();

            int32_t hour = timestamp_obj.hour();
            int32_t minute = timestamp_obj.minute();
            int32_t second = timestamp_obj.second();
            int32_t nano = timestamp_obj.nano();

            DateADT date = date2j(year, month, day);

            int64_t microseconds = ((int64_t) hour * USECS_PER_HOUR) +
                                    ((int64_t) minute * USECS_PER_MINUTE) +
                                    ((int64_t) second * USECS_PER_SEC) +
                                    ((int64_t) nano / 1000);

            // Assign to TimestampTz (microseconds since epoch)
            // Note: This simplistic conversion assumes date2j returns days since a fixed epoch
            // Adjust the calculation based on the actual definition of date2j and TimestampTz
            TimestampTz timestamp_value = ((TimestampTz) date * USECS_PER_DAY) + microseconds;

            *datum = TimestampTzGetDatum(timestamp_value);
        } else if (value.has_interval()) {
            auto interval_obj = value.interval();

            Interval* v = (Interval*) palloc(sizeof(Interval));

            v->time = ((int64_t) interval_obj.hours() * USECS_PER_HOUR) +
                    ((int64_t) interval_obj.minutes() * USECS_PER_MINUTE) +
                    ((int64_t) interval_obj.seconds() * USECS_PER_SEC) +
                    ((int64_t) interval_obj.millis() * USECS_PER_MSEC);

            v->day = interval_obj.days();
            v->month = interval_obj.months();
            // TODO (msb): This is broken. Does not handle weeks or years!
            // v->year = interval_obj.years();

            *datum = PointerGetDatum(v);
        } else if (value.has_record()) {
            JsonbParseState *state = NULL;
            JsonbValue *res = ValueToJsonbValue(value, &state);
            Jsonb *jsonb = JsonbValueToJsonb(res);
            *datum = JsonbPGetDatum(jsonb);
        } else if (value.has_list()) {
            *datum = ConvertValueListToArray(value.list());
        } else {
            elog(ERROR, "Unsupported value type: %d", value.value_case());
        }
    }
}

Datum ConvertValueListToArray(const ValueList& value_list)
{
    int nelems = value_list.values_size();

    // Default to text[] for empty arrays
    if (nelems == 0) {
        ArrayType *empty_array = construct_empty_array(TEXTOID);
        return PointerGetDatum(empty_array);
    }

    // Determine the element type based on the first element
    Oid elem_type = InvalidOid;
    int16 elem_len;
    bool elem_byval;
    char elem_align;

    const Value& first_value = value_list.values(0);

    // Map the first value's type to a PostgreSQL type Oid
    if (first_value.has_byte() || first_value.has_short_()) {
        elem_type = INT2OID;
    } else if (first_value.has_int_()) {
        elem_type = INT4OID;
    } else if (first_value.has_long_()) {
        elem_type = INT8OID;
    } else if (first_value.has_float_()) {
        elem_type = FLOAT4OID;
    } else if (first_value.has_double_()) {
        elem_type = FLOAT8OID;
    } else if (first_value.has_decimal()) {
        elem_type = NUMERICOID;
    } else if (first_value.has_string()) {
        elem_type = TEXTOID;
    } else if (first_value.has_bool_()) {
        elem_type = BOOLOID;
    } else if (first_value.has_binary()) {
        elem_type = BYTEAOID;
    } else if (first_value.has_date()) {
        elem_type = DATEOID;
    } else if (first_value.has_time()) {
        elem_type = TIMEOID;
    } else if (first_value.has_timestamp()) {
        elem_type = TIMESTAMPOID;
    } else if (first_value.has_interval()) {
        elem_type = INTERVALOID;
    } else if (first_value.has_null()) {
        // If the first element is null, default to text[]
        elem_type = TEXTOID;
    } else {
        elog(ERROR, "Unsupported or unrecognized type in list");
    }

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

JsonbValue* ValueToJsonbValue(const Value& value, JsonbParseState **pstate)
{
    if (value.has_null()) {
        JsonbValue v;
        v.type = jbvNull;
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_byte()) {
        Datum numDatum = DirectFunctionCall1(int2_numeric, Int16GetDatum(value.byte().v()));
        Numeric num = DatumGetNumeric(numDatum);
        JsonbValue v;
        v.type = jbvNumeric;
        v.val.numeric = num;
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_short_()) {
        Datum numDatum = DirectFunctionCall1(int2_numeric, Int16GetDatum(value.short_().v()));
        Numeric num = DatumGetNumeric(numDatum);
        JsonbValue v;
        v.type = jbvNumeric;
        v.val.numeric = num;
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_int_()) {
        Datum numDatum = DirectFunctionCall1(int4_numeric, Int32GetDatum(value.int_().v()));
        Numeric num = DatumGetNumeric(numDatum);
        JsonbValue v;
        v.type = jbvNumeric;
        v.val.numeric = num;
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_long_()) {
        Datum numDatum = DirectFunctionCall1(int8_numeric, Int64GetDatum(value.long_().v()));
        Numeric num = DatumGetNumeric(numDatum);
        JsonbValue v;
        v.type = jbvNumeric;
        v.val.numeric = num;
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_float_()) {
        Datum numDatum = DirectFunctionCall1(float4_numeric, Float4GetDatum(value.float_().v()));
        Numeric num = DatumGetNumeric(numDatum);
        JsonbValue v;
        v.type = jbvNumeric;
        v.val.numeric = num;
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_double_()) {
        Datum numDatum = DirectFunctionCall1(float8_numeric, Float8GetDatum(value.double_().v()));
        Numeric num = DatumGetNumeric(numDatum);
        JsonbValue v;
        v.type = jbvNumeric;
        v.val.numeric = num;
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_decimal()) {
        Datum numDatum = DirectFunctionCall1(numeric_in, CStringGetDatum(value.decimal().v().c_str()));
        Numeric num = DatumGetNumeric(numDatum);
        JsonbValue v;
        v.type = jbvNumeric;
        v.val.numeric = num;
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_string()) {
        JsonbValue v;
        v.type = jbvString;
        std::string str = value.string().v();
        v.val.string.val = pstrdup(str.c_str());
        v.val.string.len = str.length();
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_bool_()) {
        JsonbValue v;
        v.type = jbvBool;
        v.val.boolean = value.bool_().v();
        return pushJsonbValue(pstate, WJB_VALUE, &v);
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

        JsonbValue v;
        v.type = jbvString;
        v.val.string.val = encoded;
        v.val.string.len = actual_encoded_len;
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_date()) {
        auto date_obj = value.date();
        char buf[11]; // YYYY-MM-DD\0
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d", date_obj.year(), date_obj.month(), date_obj.day());
        JsonbValue v;
        v.type = jbvString;
        v.val.string.val = pstrdup(buf);
        v.val.string.len = strlen(buf);
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_time()) {
        auto time_obj = value.time();
        char buf[13]; // HH:MM:SS\0
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d", time_obj.hour(), time_obj.minute(), time_obj.second());
        JsonbValue v;
        v.type = jbvString;
        v.val.string.val = pstrdup(buf);
        v.val.string.len = strlen(buf);
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_timestamp()) {
        auto timestamp_obj = value.timestamp();
        char buf[30]; // YYYY-MM-DDTHH:MM:SS.ssssssZ\0
        snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ",
                 timestamp_obj.year(), timestamp_obj.month(), timestamp_obj.day(),
                 timestamp_obj.hour(), timestamp_obj.minute(), timestamp_obj.second(),
                 timestamp_obj.nano() / 1000);
        JsonbValue v;
        v.type = jbvString;
        v.val.string.val = pstrdup(buf);
        v.val.string.len = strlen(buf);
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_interval()) {
        auto interval_obj = value.interval();
        std::ostringstream oss;
        oss << "P";
        if (interval_obj.years() != 0) oss << interval_obj.years() << "Y";
        if (interval_obj.months() != 0) oss << interval_obj.months() << "M";
        if (interval_obj.weeks() != 0) oss << interval_obj.weeks() << "W";
        if (interval_obj.days() != 0) oss << interval_obj.days() << "D";
        if (interval_obj.hours() != 0 || interval_obj.minutes() != 0 || interval_obj.seconds() != 0 || interval_obj.millis() != 0) {
            oss << "T";
            if (interval_obj.hours() != 0) oss << interval_obj.hours() << "H";
            if (interval_obj.minutes() != 0) oss << interval_obj.minutes() << "M";
            if (interval_obj.seconds() != 0 || interval_obj.millis() != 0) {
                double seconds = interval_obj.seconds() + interval_obj.millis() / 1000.0;
                oss << seconds << "S";
            }
        }
        std::string str = oss.str();
        JsonbValue v;
        v.type = jbvString;
        v.val.string.val = pstrdup(str.c_str());
        v.val.string.len = str.length();
        return pushJsonbValue(pstate, WJB_VALUE, &v);
    } else if (value.has_record()) {
        elog(WARNING, "ValueToJsonbValue: record 1");
        pushJsonbValue(pstate, WJB_BEGIN_OBJECT, NULL);
        elog(WARNING, "ValueToJsonbValue: record 3");
        for (const auto& field : value.record().fields()) {
            JsonbValue key;
            elog(WARNING, "ValueToJsonbValue: record 3");
            key.type = jbvString;
            elog(WARNING, "ValueToJsonbValue: record 4 %s", field.name().c_str());
            key.val.string.val = pstrdup(field.name().c_str());
            key.val.string.len = field.name().length();
            pushJsonbValue(pstate, WJB_KEY, &key);
            elog(WARNING, "ValueToJsonbValue: record 5");

            ValueToJsonbValue(field.value(), pstate);
            elog(WARNING, "ValueToJsonbValue: record 6");
        }
        elog(WARNING, "ValueToJsonbValue: record 7");
        return pushJsonbValue(pstate, WJB_END_OBJECT, NULL);

    } else if (value.has_list()) {
        pushJsonbValue(pstate, WJB_BEGIN_ARRAY, NULL);

        for (const auto& item : value.list().values()) {
            ValueToJsonbValue(item, pstate);
        }

        return pushJsonbValue(pstate, WJB_END_ARRAY, NULL);
    } else {
        elog(ERROR, "Unsupported value type: %d", value.value_case());
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
    std::unique_ptr<QueryService::Stub> stub;
    grpc::ClientContext context;
    std::unique_ptr<grpc::ClientReader<Rows>> reader;
    Rows current_rows;            // Buffer for current Rows message
    int current_row_index;        // Index within current_rows
    mysql_opt* opts;
    char* sql;
    bool started;
};

// Expose the functions to C
extern "C" {

void wait_for_server(mysql_opt* opts)
{

}

//////////////////////////////////////////////////////////////////////////////////////////
// Registration Service
//////////////////////////////////////////////////////////////////////////////////////////

char* register_das(mysql_opt* opts)
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

void unregister_das(mysql_opt* opts)
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

char** get_operations_supported(mysql_opt* opts, bool* orderby_supported, bool* join_supported, bool* aggregation_supported, int* pushability_len)
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

    *orderby_supported = response.orderbysupported();
    *join_supported = response.joinsupported();
    *aggregation_supported = response.aggregationsupported();
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
        std::string operator_str = "OPERATOR pg_catalog." + TypeToOperator(op.operator_().type())
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

char** get_table_definitions(mysql_opt* opts, const char* server_name, int* num_tables)
{
    elog(DEBUG3, "Getting table definitions for DAS with URL: %s, id: %s, server: %s", opts->das_url, opts->das_id, server_name);

    auto client = TablesService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    GetTableDefinitionsRequest request;
    request.mutable_dasid()->set_id(opts->das_id);

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

void get_query_estimate(mysql_opt* opts, const char* sql, double* rows, double* width)
{
    elog(WARNING, "Getting query estimate for DAS with URL: %s, id: %s, SQL: %s", opts->das_url, opts->das_id, sql);

    auto client = QueryService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    QueryRequest request;
    DASId* dasId = request.mutable_dasid();
    dasId->set_id(opts->das_id);
    request.set_sql(sql);

    GetQueryEstimateResponse response;
    ClientContext context;
    Status status = client->GetQueryEstimate(&context, request, &response);

    if (!status.ok())
        elog(ERROR, "Failed to get query estimate: %s", status.error_message().c_str());

    *rows = response.rows();
    *width = response.bytes();

    elog(WARNING, "Got query estimate: rows: %f, width: %f", *rows, *width);
}

SqlQueryIterator* sql_query_iterator_init(mysql_opt* opts, const char* sql, const char* plan_id)
{
    elog(WARNING, "Initializing SQL query iterator for DAS with URL: %s, das_id: %s, plan_id: %s, SQL: %s", opts->das_url, opts->das_id, plan_id, sql);

    void *mem = palloc(sizeof(SqlQueryIterator));
    auto iterator = new (mem) SqlQueryIterator();
    iterator->stub = QueryService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));
    iterator->opts = opts;
    iterator->sql = pstrdup(sql);
    iterator->started = false;
    QueryRequest request;
    DASId* dasId = request.mutable_dasid();
    dasId->set_id(opts->das_id);
    request.set_sql(sql);
    request.set_planid(plan_id);
    iterator->reader = iterator->stub->ExecuteQuery(&iterator->context, request);
    iterator->current_row_index = 0;
    return iterator;
}

bool sql_query_iterator_next(SqlQueryIterator* iterator, int* attnums, Datum* dvalues, bool* nulls, Oid* pgtypes, int32* pgtypmods)
{
    elog(WARNING, "Fetching next row");

    // Check if we need to read another chunk of rows
    if (iterator->current_row_index >= iterator->current_rows.rows_size())
    {
        if (!iterator->reader->Read(&iterator->current_rows))
        {
            grpc::Status status = iterator->reader->Finish();
        
            if (!status.ok()) {
                if (!iterator->started)
                {
                    // Wait for the server to be available
                    if (status.error_code() == grpc::StatusCode::UNAVAILABLE)
                        wait_for_server(iterator->opts);                
                    elog(WARNING, "bad status1");
                    // Retry if the DAS is not found
                    if (iterator->opts->das_id && status.error_message().find("DAS not found") != std::string::npos) {
                        elog(WARNING, "bad status2");
                        register_das(iterator->opts);
                        elog(WARNING, "bad status3");
                        QueryRequest request;
                        DASId* dasId = request.mutable_dasid();
                        dasId->set_id(iterator->opts->das_id);
                        request.set_sql(iterator->sql);
                        elog(WARNING, "bad status4 %s das_id %s", iterator->opts->das_url, iterator->opts->das_id);
                        iterator->stub = QueryService::NewStub(grpc::CreateChannel(iterator->opts->das_url, grpc::InsecureChannelCredentials()));
                        elog(WARNING, "bad status5");
                        
                        iterator->context.~ClientContext();  // Destroy the old context
                        new (&iterator->context) grpc::ClientContext();  // Reconstruct a new context in-place

                        iterator->reader = iterator->stub->ExecuteQuery(&iterator->context, request);
                        elog(WARNING, "bad status6");
                        return sql_query_iterator_next(iterator, attnums, dvalues, nulls, pgtypes, pgtypmods);
                    }
                }

                elog(ERROR, "gRPC stream failed with error: %s, code: %d", status.error_message().c_str(), status.error_code());
            }

            elog(DEBUG1, "No more data");
            return false;
        }
        iterator->started = true;
        iterator->current_row_index = 0;

        if (iterator->current_rows.rows_size() == 0)
        {
            elog(WARNING, "Fetched next Rows message with 0 rows");
            return false;
        }
    }

    // Get the current row
    const Row& row = iterator->current_rows.rows(iterator->current_row_index++);
    const auto& columns = row.columns();

    int num_columns = columns.size();
    for (int i = 0; i < num_columns; ++i)
    {
        const Column& column = columns[i];
        const Value& value = column.data();

        elog(DEBUG3, "Processing field %d with %s", i, column.name().c_str());

        int attnum = attnums[i];

        ValueToDatum(value, pgtypes[attnum], pgtypmods[attnum], &dvalues[attnum], &nulls[attnum]);
    }

    return true;
}

void sql_query_iterator_close(SqlQueryIterator* iterator)
{
    elog(DEBUG3, "Closing SQL query iterator");

    com::rawlabs::protocol::das::Rows response;

    // // TODO (msb): This is a hack to drain the iterator. It is VERY expensive.
    // while (iterator->reader->Read(&response))
    //     // Optionally process the response or discard it
    //     ;

    // grpc::Status status = iterator->reader->Finish();
    // if (!status.ok())
    //     elog(ERROR, "gRPC error: %s", status.error_message().c_str());

    iterator->~SqlQueryIterator();
    pfree(iterator);
}

//////////////////////////////////////////////////////////////////////////////////////////
// Table Update Service
//////////////////////////////////////////////////////////////////////////////////////////

char* unique_column(mysql_opt* opts, const char* table_name)
{
    elog(DEBUG3, "Getting unique column for DAS with URL: %s, id: %s, table: %s", opts->das_url, opts->das_id, table_name);

    auto client = TablesService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    UniqueColumnRequest request;

    DASId* dasId = request.mutable_dasid();
    dasId->set_id(opts->das_id);

    TableId* tableId = request.mutable_tableid();
    tableId->set_name(table_name);

    UniqueColumnResponse response;
    ClientContext context;
    Status status = client->UniqueColumn(&context, request, &response);

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

void insert_row(mysql_opt* opts, const char* table_name, int num_columns, int* attnums, char **attnames, Datum* dvalues, bool* nulls, Oid* pgtypes, int32* pgtypmods)
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
    InsertRequest request;
    request.mutable_dasid()->set_id(opts->das_id);
    elog(WARNING, "Sending insert request 2");
    request.mutable_tableid()->set_name(std::string(table_name));
    elog(WARNING, "Sending insert request 3");
    request.mutable_values()->CopyFrom(row);
    elog(WARNING, "Sending insert request 4");

    InsertResponse response;
    ClientContext context;
    elog(WARNING, "Sending insert request 5");
    Status status = client->Insert(&context, request, &response);
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

void update_row(mysql_opt* opts, const char* table_name, Datum k_value, Oid k_pgtype, int32 k_pgtypmods, int num_columns, int* attnums, char **attnames, Datum* dvalues, bool* nulls, Oid* pgtypes, int32* pgtypmods)
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

    UpdateRequest request;
    request.mutable_dasid()->set_id(opts->das_id);
    request.mutable_tableid()->set_name(std::string(table_name));
    request.mutable_rowid()->CopyFrom(key_datum);
    request.mutable_newvalues()->CopyFrom(row);

    UpdateResponse response;
    ClientContext context;
    Status status = client->Update(&context, request, &response);

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

void delete_row(mysql_opt* opts, const char* table_name, Datum k_value, Oid k_pgtype, int32 k_pgtypmods)
{
    elog(DEBUG3, "Deleting row from table %s for DAS with URL: %s, id: %s", table_name, opts->das_url, opts->das_id);

    auto client = TablesService::NewStub(grpc::CreateChannel(opts->das_url, grpc::InsecureChannelCredentials()));

    Value key_datum = DatumToValue(k_value, k_pgtype, k_pgtypmods);

    DeleteRequest request;
    request.mutable_dasid()->set_id(opts->das_id);
    request.mutable_tableid()->set_name(std::string(table_name));
    request.mutable_rowid()->CopyFrom(key_datum);

    DeleteResponse response;
    ClientContext context;
    Status status = client->Delete(&context, request, &response);

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