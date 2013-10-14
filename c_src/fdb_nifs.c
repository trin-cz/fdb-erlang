#include <string.h>
#include "erl_nif.h"
#include "nif_resources.h"
#include "fdb_c.h"

static enif_network_t *network = NULL;

static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_not_found;
static ERL_NIF_TERM atom_undefined;
static ERL_NIF_TERM error_not_implemented;

static ERL_NIF_TERM mk_and_release_resource(ErlNifEnv* env,void *resptr)
{
    ERL_NIF_TERM res;
    res = enif_make_resource(env,resptr);
    enif_release_resource(resptr);
    return res;
}

static ERL_NIF_TERM mk_error(ErlNifEnv* env,const char* error)
{
    return enif_make_tuple2(env,atom_error,enif_make_atom(env,error));
}

static ERL_NIF_TERM mk_result(ErlNifEnv* env, fdb_error_t errcode, ERL_NIF_TERM result)
{
    return enif_make_tuple2(env,enif_make_int(env,errcode),result);
}

/*
 * This function is called when loading the nif.
 *
 * it registers the different resource types that are being used by the library.
 */
static int nif_on_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    atom_ok = enif_make_atom(env, "ok");
    atom_error = enif_make_atom(env, "error");
    atom_not_found = enif_make_atom(env,"not_found");
    atom_undefined = enif_make_atom(env,"undefined");
    error_not_implemented = mk_error(env,"not_implemented");

    if (register_fdb_resources(env)!=0)
        return -1;

    network = wrap_network();

    enif_make_resource(env,(void*)network);

    return 0;
}

/*
 * Start FDB API wrapper.
 */

static ERL_NIF_TERM nif_fdb_cluster_configure_database(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_cluster_create_database(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_cluster_t *cluster;
    if (argc!=1 ||
            get_cluster(env,argv[0],&cluster)==0
       ) return enif_make_badarg(env);

    FDBFuture* future  =
        fdb_cluster_create_database(cluster->handle,(const uint8_t*)"DB",2);

    return mk_and_release_resource(env, wrap_future(future));
}

static ERL_NIF_TERM nif_fdb_cluster_create_database_2(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=2) return enif_make_badarg(env);

    //  FDBCluster* c;
    //  uint8_t const* db_name;
    //  int db_name_length;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_cluster_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBCluster* c;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_cluster_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=4) return enif_make_badarg(env);

    //  FDBCluster* c;
    //  FDBClusterOption option;
    //  uint8_t const* value;
    //  int value_length;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_create_cluster(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);


    return mk_and_release_resource(env,wrap_future(fdb_create_cluster(NULL)));
}

static ERL_NIF_TERM nif_fdb_create_cluster_1(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  const char* cluster_file_path;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_database_create_transaction(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_database_t *DB;
    enif_transaction_t *Tx = wrap_transaction(NULL);

    if (argc!=1 ||
            get_database(env, argv[0],&DB) == 0
       ) return enif_make_badarg(env);

    fdb_error_t err = fdb_database_create_transaction(DB->handle, &Tx->handle);

    return mk_result(env, err,mk_and_release_resource(env,Tx));
}

static ERL_NIF_TERM nif_fdb_database_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

    if (argc!=1) return enif_make_badarg(env);

    //  FDBDatabase* d;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_database_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=4) return enif_make_badarg(env);

    //  FDBDatabase* d;
    //  FDBDatabaseOption option;
    //  uint8_t const* value;
    //  int value_length;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_block_until_ready(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    //FDBFuture* f;
    enif_future_t *f;
    if (argc!=1 ||
            get_future(env,argv[0],&f) == 0
       )
        return enif_make_badarg(env);

    return enif_make_int(env,fdb_future_block_until_ready(f->handle));
}

static ERL_NIF_TERM nif_fdb_future_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBFuture *f;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBFuture* f;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_get_cluster(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    //FDBFuture* f;
    //FDBCluster** out_cluster;
    enif_future_t *f;
    enif_cluster_t *out_cluster = wrap_cluster(NULL);
    if (argc!=1|| get_future(env,argv[0],&f) == 0)
        return enif_make_badarg(env);

    int err =fdb_future_get_cluster(f->handle,&out_cluster->handle);

    return mk_result(env,err,mk_and_release_resource(env,out_cluster));
}

static ERL_NIF_TERM nif_fdb_future_get_database(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *f;
    enif_database_t *out_database=wrap_database(NULL);
    if (argc!=1|| get_future(env,argv[0],&f) == 0 )
        return enif_make_badarg(env);

    int err =fdb_future_get_database(f->handle,&out_database->handle);

    return mk_result(env,err,mk_and_release_resource(env,out_database));
}

static ERL_NIF_TERM nif_fdb_future_get_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *Future;
    if (argc!=1) return enif_make_badarg(env);
    if (get_future(env,argv[0],&Future) == 0) return enif_make_badarg(env);

    return enif_make_int(env,fdb_future_get_error(Future->handle));
}

static ERL_NIF_TERM nif_fdb_future_get_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=3) return enif_make_badarg(env);

    //  FDBFuture* f;
    //  uint8_t const** out_key;
    //  int* out_key_length;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_get_keyvalue_array(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=4) return enif_make_badarg(env);

    //  FDBFuture* f;
    //  FDBKeyValue const** out_kv;
    //  int* out_count;
    //  fdb_bool_t* out_more;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_get_string_array(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=3) return enif_make_badarg(env);

    // FDBFuture* f;
    //  const char*** out_strings;
    //  int* out_count)

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_get_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *Future;
    fdb_bool_t out_present;
    const uint8_t *data;
    int size;

    if (argc!=1) return enif_make_badarg(env);
    if (get_future(env,argv[0],&Future) == 0) return enif_make_badarg(env);

    fdb_error_t err = fdb_future_get_value(Future->handle, &out_present,&data, &size);

    if (err != 0)
    {
        return mk_result(env,err,atom_undefined);
    }
    if (out_present == 0)
    {
        return mk_result(env,err,atom_not_found);
    }
    else
    {
        ERL_NIF_TERM result;

        uint8_t *chars = enif_make_new_binary(env,size,&result);
        memcpy(chars,data,size);

        return mk_result(env,err,result);
    }
}

static ERL_NIF_TERM nif_fdb_future_get_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=2) return enif_make_badarg(env);

    //  FDBFuture* f;
    //  int64_t* out_version;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_is_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBFuture* f;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_is_ready(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBFuture* f;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_release_memory(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBFuture* f;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_future_set_callback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=3) return enif_make_badarg(env);

    //  FDBFuture* f;
    //  FDBCallback callback;
    //  void* callback_parameter;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_get_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    fdb_error_t code;
    if ( argc!=1 ||
            enif_get_int(env, argv[0],&code)== 0
       )
        return enif_make_badarg(env);

    return enif_make_atom(env,fdb_get_error(code));
}

static ERL_NIF_TERM nif_fdb_get_max_api_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    // )

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_network_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=3) return enif_make_badarg(env);

    //  FDBNetworkOption option;
    //  uint8_t const* value;
    //  int value_length;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_run_network(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);
    if (network->is_running) return mk_error(env,"network_already_running");
    pthread_create( &(network->thread),NULL, (void* (*)(void*))fdb_run_network,NULL);
    enif_make_resource(env,network);
    network->is_running = 1;
    return atom_ok;
}

static ERL_NIF_TERM nif_fdb_select_api_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int runtime_version;
    if (argc!=1 ||
            enif_get_int(env,argv[0],&runtime_version)==0
       )
        return enif_make_badarg(env);

    return enif_make_int(env,fdb_select_api_version(runtime_version));
}

static ERL_NIF_TERM nif_fdb_setup_network(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    return enif_make_int(env,fdb_setup_network());
}

static ERL_NIF_TERM nif_fdb_stop_network(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    if (network->is_running==0) return enif_make_int(env,-1);

    enif_release_resource(network);

    return enif_make_int(env,0);
}

static ERL_NIF_TERM nif_fdb_transaction_add_conflict_range(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=6) return enif_make_badarg(env);

    // FDBTransaction *tr;
    //  uint8_t const* begin_key_name;
    //  int begin_key_name_length;
    //  uint8_t const* end_key_name;
    //  int end_key_name_length;
    //  FDBConflictRangeType type)

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_atomic_op(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=6) return enif_make_badarg(env);

    //  FDBTransaction* tr;
    //  uint8_t const* key_name;
    //  int key_name_length;
    //  uint8_t const* param;
    //  int param_length;
    //  FDBMutationType operation_type;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBTransaction* tr)

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_clear(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary Key;

    if (argc!=2) return enif_make_badarg(env);
    if (get_transaction(env,argv[0],&Tx) == 0) return enif_make_badarg(env);
    if (enif_inspect_binary(env,argv[1],&Key) == 0
            && enif_inspect_iolist_as_binary(env,argv[2], &Key) == 0) return enif_make_badarg(env);

    fdb_transaction_clear(Tx->handle,Key.data,Key.size);

    return atom_ok;

}

static ERL_NIF_TERM nif_fdb_transaction_clear_range(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=5) return enif_make_badarg(env);

    //  FDBTransaction* tr;
    //  uint8_t const* begin_key_name;
    //  int begin_key_name_length;
    //  uint8_t const* end_key_name;
    //  int end_key_name_length;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;

    if (argc!=1) return enif_make_badarg(env);
    if (get_transaction(env,argv[0],&Tx)==0) return enif_make_badarg(env);

    return mk_and_release_resource(env,wrap_future(fdb_transaction_commit(Tx->handle)));
}

static ERL_NIF_TERM nif_fdb_transaction_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBTransaction* tr)

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary Key;
    enif_future_t *Future = wrap_future(NULL);

    if (argc!=2) return enif_make_badarg(env);
    if (get_transaction(env,argv[0],&Tx) == 0) return enif_make_badarg(env);
    if (enif_inspect_binary(env,argv[1],&Key) == 0
            && enif_inspect_iolist_as_binary(env,argv[2], &Key) == 0) return enif_make_badarg(env);

    Future->handle = fdb_transaction_get(Tx->handle,Key.data,Key.size,0);

    return mk_and_release_resource(env,Future);

}

static ERL_NIF_TERM nif_fdb_transaction_get_addresses_for_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=3) return enif_make_badarg(env);

    // FDBTransaction* tr;
    //  uint8_t const* key_name;
    //  int key_name_length)

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_get_committed_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=2) return enif_make_badarg(env);

    //  FDBTransaction* tr;
    //  int64_t* out_version;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_get_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=6) return enif_make_badarg(env);

    //  FDBTransaction* tr;
    //  uint8_t const* key_name;
    //  int key_name_length;
    //  fdb_bool_t or_equal;
    //  int offset;
    //  fdb_bool_t snapshot;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_get_range(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=15) return mk_error(env,"expected_15_parameters");

    //  FDBTransaction* tr;
    //  uint8_t const* begin_key_name;
    //  int begin_key_name_length;
    //  fdb_bool_t begin_or_equal;
    //  int begin_offset;
    //  uint8_t const* end_key_name;
    //  int end_key_name_length;
    //  fdb_bool_t end_or_equal;
    //  int end_offset;
    //  int limit;
    //  int target_bytes;
    //  FDBStreamingMode mode;
    //  int iteration;
    //  fdb_bool_t snapshot;
    //  fdb_bool_t reverse;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_get_range_selector(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=10) return mk_error(env,"expected_10_parameters");

    //  FDBTransaction* tr;
    //  uint8_t const* begin_key_name;
    //  int begin_key_name_length;
    //  fdb_bool_t begin_or_equal;
    //  int begin_offset;
    //  uint8_t const* end_key_name;
    //  int end_key_name_length;
    //  fdb_bool_t end_or_equal;
    //  int end_offset;
    //  int limit;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_get_read_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBTransaction* tr;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_on_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

    enif_transaction_t* Tx;
    fdb_error_t error;
    if (argc!=2) return enif_make_badarg(env);
    if (get_transaction(env,argv[0],&Tx)==0) return enif_make_badarg(env);
    if (enif_get_int(env,argv[1],&error)==0) return enif_make_badarg(env);

    
    return mk_and_release_resource(env,wrap_future(fdb_transaction_on_error(Tx->handle,error)));
}

static ERL_NIF_TERM nif_fdb_transaction_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=1) return enif_make_badarg(env);

    //  FDBTransaction* tr;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_set(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary Key,Value;
    if (argc!=3) return enif_make_badarg(env);
    if (get_transaction(env,argv[0], &Tx) == 0) return enif_make_badarg(env);
    if (enif_inspect_binary(env,argv[1],&Key) == 0
            && enif_inspect_iolist_as_binary(env,argv[1],&Key) == 0) return enif_make_badarg(env);
    if (enif_inspect_binary(env,argv[2],&Value) == 0
            && enif_inspect_iolist_as_binary(env,argv[2],&Value) == 0) return enif_make_badarg(env);

    fdb_transaction_set(Tx->handle,Key.data, Key.size, Value.data, Value.size);

    return atom_ok;
}

static ERL_NIF_TERM nif_fdb_transaction_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=4) return enif_make_badarg(env);

    //  FDBTransaction* tr;
    //  FDBTransactionOption option;
    //  uint8_t const* value;
    //  int value_length;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_set_read_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=2) return enif_make_badarg(env);

    //  FDBTransaction* tr;
    //  int64_t version;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_watch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=3) return enif_make_badarg(env);

    //  FDBTransaction *tr;
    //  uint8_t const* key_name;
    //  int key_name_length)

    return error_not_implemented;
}

static ERL_NIF_TERM nif_new_cluster(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    return mk_and_release_resource(env,wrap_cluster(NULL));
}

static ERL_NIF_TERM nif_new_database(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    return mk_and_release_resource(env,wrap_database(NULL));
}

static ERL_NIF_TERM nif_new_transaction(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    return mk_and_release_resource(env,wrap_transaction(NULL));
}

static ErlNifFunc nifs[] =
{
    {"fdb_cluster_configure_database", 0, nif_fdb_cluster_configure_database},
    {"fdb_cluster_create_database", 1, nif_fdb_cluster_create_database},
    {"fdb_cluster_create_database", 2, nif_fdb_cluster_create_database_2},
    {"fdb_cluster_destroy", 1, nif_fdb_cluster_destroy},
    {"fdb_cluster_set_option", 4, nif_fdb_cluster_set_option},
    {"fdb_create_cluster", 0, nif_fdb_create_cluster},
    {"fdb_create_cluster", 1, nif_fdb_create_cluster_1},
    {"fdb_database_create_transaction", 1, nif_fdb_database_create_transaction},
    {"fdb_database_destroy", 1, nif_fdb_database_destroy},
    {"fdb_database_set_option", 4, nif_fdb_database_set_option},
    {"fdb_future_block_until_ready", 1, nif_fdb_future_block_until_ready},
    {"fdb_future_cancel", 1, nif_fdb_future_cancel},
    {"fdb_future_destroy", 1, nif_fdb_future_destroy},
    {"fdb_future_get_cluster", 1, nif_fdb_future_get_cluster},
    {"fdb_future_get_database", 1, nif_fdb_future_get_database},
    {"fdb_future_get_error", 1, nif_fdb_future_get_error},
    {"fdb_future_get_key", 3, nif_fdb_future_get_key},
    {"fdb_future_get_keyvalue_array", 4, nif_fdb_future_get_keyvalue_array},
    {"fdb_future_get_string_array", 3, nif_fdb_future_get_string_array},
    {"fdb_future_get_value", 1, nif_fdb_future_get_value},
    {"fdb_future_get_version", 2, nif_fdb_future_get_version},
    {"fdb_future_is_error", 1, nif_fdb_future_is_error},
    {"fdb_future_is_ready", 1, nif_fdb_future_is_ready},
    {"fdb_future_release_memory", 1, nif_fdb_future_release_memory},
    {"fdb_future_set_callback", 3, nif_fdb_future_set_callback},
    {"fdb_get_error", 1, nif_fdb_get_error},
    {"fdb_get_max_api_version", 1, nif_fdb_get_max_api_version},
    {"fdb_network_set_option", 3, nif_fdb_network_set_option},
    {"fdb_run_network", 0, nif_fdb_run_network},
    {"fdb_select_api_version", 1, nif_fdb_select_api_version},
    {"fdb_setup_network", 0, nif_fdb_setup_network},
    {"fdb_stop_network", 0, nif_fdb_stop_network},
    {"fdb_transaction_add_conflict_range", 6, nif_fdb_transaction_add_conflict_range},
    {"fdb_transaction_atomic_op", 6, nif_fdb_transaction_atomic_op},
    {"fdb_transaction_cancel", 1, nif_fdb_transaction_cancel},
    {"fdb_transaction_clear", 2, nif_fdb_transaction_clear},
    {"fdb_transaction_clear_range", 5, nif_fdb_transaction_clear_range},
    {"fdb_transaction_commit", 1, nif_fdb_transaction_commit},
    {"fdb_transaction_destroy", 1, nif_fdb_transaction_destroy},
    {"fdb_transaction_get", 2, nif_fdb_transaction_get},
    {"fdb_transaction_get_addresses_for_key", 3, nif_fdb_transaction_get_addresses_for_key},
    {"fdb_transaction_get_committed_version", 2, nif_fdb_transaction_get_committed_version},
    {"fdb_transaction_get_key", 6, nif_fdb_transaction_get_key},
    {"fdb_transaction_get_range", 15, nif_fdb_transaction_get_range},
    {"fdb_transaction_get_range_selector", 10, nif_fdb_transaction_get_range_selector},
    {"fdb_transaction_get_read_version", 1, nif_fdb_transaction_get_read_version},
    {"fdb_transaction_on_error", 2, nif_fdb_transaction_on_error},
    {"fdb_transaction_reset", 1, nif_fdb_transaction_reset},
    {"fdb_transaction_set", 3, nif_fdb_transaction_set},
    {"fdb_transaction_set_option", 4, nif_fdb_transaction_set_option},
    {"fdb_transaction_set_read_version", 2, nif_fdb_transaction_set_read_version},
    {"fdb_transaction_watch", 3, nif_fdb_transaction_watch},
    {"new_cluster", 0, nif_new_cluster},
    {"new_database", 0, nif_new_database},
    {"new_transaction", 0, nif_new_transaction}
};


ERL_NIF_INIT(fdb_nif,nifs,nif_on_load,NULL,NULL,NULL)
