-ifndef(APBCOUNTERUPDATE_PB_H).
-define(APBCOUNTERUPDATE_PB_H, true).
-record(apbcounterupdate, {
    inc
}).
-endif.

-ifndef(APBGETCOUNTERRESP_PB_H).
-define(APBGETCOUNTERRESP_PB_H, true).
-record(apbgetcounterresp, {
    value = erlang:error({required, value})
}).
-endif.

-ifndef(APBBCOUNTERUPDATE_PB_H).
-define(APBBCOUNTERUPDATE_PB_H, true).
-record(apbbcounterupdate, {
    inc,
    actor = erlang:error({required, actor}),
    transferdest
}).
-endif.

-ifndef(APBGETBCOUNTERRESP_PB_H).
-define(APBGETBCOUNTERRESP_PB_H, true).
-record(apbgetbcounterresp, {
    incs = [],
    decs = []
}).
-endif.

-ifndef(APBBCOUNTERENTRY_PB_H).
-define(APBBCOUNTERENTRY_PB_H, true).
-record(apbbcounterentry, {
    actor = erlang:error({required, actor}),
    value = erlang:error({required, value})
}).
-endif.

-ifndef(APBSETUPDATE_PB_H).
-define(APBSETUPDATE_PB_H, true).
-record(apbsetupdate, {
    optype = erlang:error({required, optype}),
    adds = [],
    rems = []
}).
-endif.

-ifndef(APBGETSETRESP_PB_H).
-define(APBGETSETRESP_PB_H, true).
-record(apbgetsetresp, {
    value = []
}).
-endif.

-ifndef(APBREGUPDATE_PB_H).
-define(APBREGUPDATE_PB_H, true).
-record(apbregupdate, {
    value = erlang:error({required, value})
}).
-endif.

-ifndef(APBGETREGRESP_PB_H).
-define(APBGETREGRESP_PB_H, true).
-record(apbgetregresp, {
    value = erlang:error({required, value})
}).
-endif.

-ifndef(APBGETMVREGRESP_PB_H).
-define(APBGETMVREGRESP_PB_H, true).
-record(apbgetmvregresp, {
    values = []
}).
-endif.

-ifndef(APBMAPKEY_PB_H).
-define(APBMAPKEY_PB_H, true).
-record(apbmapkey, {
    key = erlang:error({required, key}),
    type = erlang:error({required, type})
}).
-endif.

-ifndef(APBMAPUPDATE_PB_H).
-define(APBMAPUPDATE_PB_H, true).
-record(apbmapupdate, {
    updates = [],
    removedkeys = []
}).
-endif.

-ifndef(APBMAPNESTEDUPDATE_PB_H).
-define(APBMAPNESTEDUPDATE_PB_H, true).
-record(apbmapnestedupdate, {
    key = erlang:error({required, key}),
    update = erlang:error({required, update})
}).
-endif.

-ifndef(APBGETMAPRESP_PB_H).
-define(APBGETMAPRESP_PB_H, true).
-record(apbgetmapresp, {
    entries = []
}).
-endif.

-ifndef(APBMAPENTRY_PB_H).
-define(APBMAPENTRY_PB_H, true).
-record(apbmapentry, {
    key = erlang:error({required, key}),
    value = erlang:error({required, value})
}).
-endif.

-ifndef(APBFLAGUPDATE_PB_H).
-define(APBFLAGUPDATE_PB_H, true).
-record(apbflagupdate, {
    value = erlang:error({required, value})
}).
-endif.

-ifndef(APBGETFLAGRESP_PB_H).
-define(APBGETFLAGRESP_PB_H, true).
-record(apbgetflagresp, {
    value = erlang:error({required, value})
}).
-endif.

-ifndef(APBINDEXPUPDATE_PB_H).
-define(APBINDEXPUPDATE_PB_H, true).
-record(apbindexpupdate, {
    updates = [],
    removes = []
}).
-endif.

-ifndef(APBINDEXPNESTEDUPDATE_PB_H).
-define(APBINDEXPNESTEDUPDATE_PB_H, true).
-record(apbindexpnestedupdate, {
    key = erlang:error({required, key}),
    update = erlang:error({required, update})
}).
-endif.

-ifndef(APBINDEXKEY_PB_H).
-define(APBINDEXKEY_PB_H, true).
-record(apbindexkey, {
    key = erlang:error({required, key})
}).
-endif.

-ifndef(APBINDEXSUPDATE_PB_H).
-define(APBINDEXSUPDATE_PB_H, true).
-record(apbindexsupdate, {
    updates = [],
    removes = []
}).
-endif.

-ifndef(APBINDEXSNESTEDUPDATE_PB_H).
-define(APBINDEXSNESTEDUPDATE_PB_H, true).
-record(apbindexsnestedupdate, {
    key = erlang:error({required, key}),
    update = []
}).
-endif.

-ifndef(APBINDEXSTUPLEOPERATION_PB_H).
-define(APBINDEXSTUPLEOPERATION_PB_H, true).
-record(apbindexstupleoperation, {
    fieldname = erlang:error({required, fieldname}),
    fieldtype = erlang:error({required, fieldtype}),
    op = erlang:error({required, op})
}).
-endif.

-ifndef(APBGETINDEXRESP_PB_H).
-define(APBGETINDEXRESP_PB_H, true).
-record(apbgetindexresp, {
    entries = []
}).
-endif.

-ifndef(APBINDEXENTRY_PB_H).
-define(APBINDEXENTRY_PB_H, true).
-record(apbindexentry, {
    key = erlang:error({required, key}),
    boundobjs = []
}).
-endif.

-ifndef(APBCRDTRESET_PB_H).
-define(APBCRDTRESET_PB_H, true).
-record(apbcrdtreset, {
    
}).
-endif.

-ifndef(APBOPERATIONRESP_PB_H).
-define(APBOPERATIONRESP_PB_H, true).
-record(apboperationresp, {
    success = erlang:error({required, success}),
    errorcode
}).
-endif.

-ifndef(APBTXNPROPERTIES_PB_H).
-define(APBTXNPROPERTIES_PB_H, true).
-record(apbtxnproperties, {
    certify,
    locks = [],
    update_clock,
    shared_locks = [],
    exclusive_locks = []
}).
-endif.

-ifndef(APBBOUNDOBJECT_PB_H).
-define(APBBOUNDOBJECT_PB_H, true).
-record(apbboundobject, {
    key = erlang:error({required, key}),
    type = erlang:error({required, type}),
    bucket = erlang:error({required, bucket})
}).
-endif.

-ifndef(APBREADOBJECTS_PB_H).
-define(APBREADOBJECTS_PB_H, true).
-record(apbreadobjects, {
    boundobjects = [],
    transaction_descriptor = erlang:error({required, transaction_descriptor})
}).
-endif.

-ifndef(APBQUERYOBJECTS_PB_H).
-define(APBQUERYOBJECTS_PB_H, true).
-record(apbqueryobjects, {
    filter = erlang:error({required, filter}),
    transaction_descriptor = erlang:error({required, transaction_descriptor})
}).
-endif.

-ifndef(APBQUERYOBJECTSRESP_PB_H).
-define(APBQUERYOBJECTSRESP_PB_H, true).
-record(apbqueryobjectsresp, {
    success = erlang:error({required, success}),
    result = erlang:error({required, result}),
    errorcode
}).
-endif.

-ifndef(APBUPDATEOP_PB_H).
-define(APBUPDATEOP_PB_H, true).
-record(apbupdateop, {
    boundobject = erlang:error({required, boundobject}),
    operation = erlang:error({required, operation})
}).
-endif.

-ifndef(APBUPDATEOPERATION_PB_H).
-define(APBUPDATEOPERATION_PB_H, true).
-record(apbupdateoperation, {
    counterop,
    bcounterop,
    setop,
    regop,
    mapop,
    resetop,
    flagop,
    indexpop,
    indexsop
}).
-endif.

-ifndef(APBUPDATEOBJECTS_PB_H).
-define(APBUPDATEOBJECTS_PB_H, true).
-record(apbupdateobjects, {
    updates = [],
    transaction_descriptor = erlang:error({required, transaction_descriptor})
}).
-endif.

-ifndef(APBSTARTTRANSACTION_PB_H).
-define(APBSTARTTRANSACTION_PB_H, true).
-record(apbstarttransaction, {
    timestamp,
    properties
}).
-endif.

-ifndef(APBABORTTRANSACTION_PB_H).
-define(APBABORTTRANSACTION_PB_H, true).
-record(apbaborttransaction, {
    transaction_descriptor = erlang:error({required, transaction_descriptor})
}).
-endif.

-ifndef(APBCOMMITTRANSACTION_PB_H).
-define(APBCOMMITTRANSACTION_PB_H, true).
-record(apbcommittransaction, {
    transaction_descriptor = erlang:error({required, transaction_descriptor})
}).
-endif.

-ifndef(APBSTATICUPDATEOBJECTS_PB_H).
-define(APBSTATICUPDATEOBJECTS_PB_H, true).
-record(apbstaticupdateobjects, {
    transaction = erlang:error({required, transaction}),
    updates = []
}).
-endif.

-ifndef(APBSTATICREADOBJECTS_PB_H).
-define(APBSTATICREADOBJECTS_PB_H, true).
-record(apbstaticreadobjects, {
    transaction = erlang:error({required, transaction}),
    objects = []
}).
-endif.

-ifndef(APBSTARTTRANSACTIONRESP_PB_H).
-define(APBSTARTTRANSACTIONRESP_PB_H, true).
-record(apbstarttransactionresp, {
    success = erlang:error({required, success}),
    transaction_descriptor,
    errorcode
}).
-endif.

-ifndef(APBREADOBJECTRESP_PB_H).
-define(APBREADOBJECTRESP_PB_H, true).
-record(apbreadobjectresp, {
    counter,
    counter_b,
    set,
    reg,
    mvreg,
    map,
    flag,
    index
}).
-endif.

-ifndef(APBREADOBJECTSRESP_PB_H).
-define(APBREADOBJECTSRESP_PB_H, true).
-record(apbreadobjectsresp, {
    success = erlang:error({required, success}),
    objects = [],
    errorcode
}).
-endif.

-ifndef(APBCOMMITRESP_PB_H).
-define(APBCOMMITRESP_PB_H, true).
-record(apbcommitresp, {
    success = erlang:error({required, success}),
    commit_time,
    errorcode
}).
-endif.

-ifndef(APBSTATICREADOBJECTSRESP_PB_H).
-define(APBSTATICREADOBJECTSRESP_PB_H, true).
-record(apbstaticreadobjectsresp, {
    objects = erlang:error({required, objects}),
    committime = erlang:error({required, committime})
}).
-endif.

