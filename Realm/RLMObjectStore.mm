////////////////////////////////////////////////////////////////////////////
//
// Copyright 2014 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#import "RLMObjectStore.h"

#import "RLMAccessor.h"
#import "RLMArray_Private.hpp"
#import "RLMListBase.h"
#import "RLMObservation.hpp"
#import "RLMObject_Private.hpp"
#import "RLMObjectSchema_Private.hpp"
#import "RLMOptionalBase.h"
#import "RLMProperty_Private.h"
#import "RLMQueryUtil.hpp"
#import "RLMRealm_Private.hpp"
#import "RLMSchema_Private.h"
#import "RLMSwiftSupport.h"
#import "RLMUtil.hpp"

#import "object_store.hpp"
#import "object_accessor.hpp"
#import "results.hpp"
#import "shared_realm.hpp"

#import <objc/message.h>

using namespace realm;

namespace {
static void validateValueForProperty(__unsafe_unretained id const obj,
                                     __unsafe_unretained RLMProperty *const prop) {
    switch (prop.type) {
        case RLMPropertyTypeString:
        case RLMPropertyTypeBool:
        case RLMPropertyTypeDate:
        case RLMPropertyTypeInt:
        case RLMPropertyTypeFloat:
        case RLMPropertyTypeDouble:
        case RLMPropertyTypeData:
            if (!RLMIsObjectValidForProperty(obj, prop)) {
                @throw RLMException(@"Invalid value '%@' for property '%@'", obj, prop.name);
            }
            break;
        case RLMPropertyTypeObject:
            break;
        case RLMPropertyTypeArray: {
            if (obj != nil && obj != NSNull.null) {
                if (![obj conformsToProtocol:@protocol(NSFastEnumeration)]) {
                    @throw RLMException(@"Array property value (%@) is not enumerable.", obj);
                }
            }
            break;
        }
        case RLMPropertyTypeAny:
        case RLMPropertyTypeLinkingObjects:
            @throw RLMException(@"Invalid value '%@' for property '%@'", obj, prop.name);
    }
}
struct Context {
    RLMRealm *realm;
    RLMClassInfo& info;
    NSDictionary *defaultValues;

    id defaultValue(NSString *key) {
        if (!defaultValues) {
            defaultValues = RLMDefaultValuesForObjectSchema(info.rlmObjectSchema);
        }
        return defaultValues[key];
    }

    id value(id obj, size_t propIndex) {
        auto prop = info.rlmObjectSchema.properties[propIndex];
        id value = doGetValue(obj, propIndex, prop);
        validateValueForProperty(value, prop);

        if ([obj isKindOfClass:info.rlmObjectSchema.objectClass]) {
            // set the ivars for object and array properties to nil as otherwise the
            // accessors retain objects that are no longer accessible via the properties
            // this is mainly an issue when the object graph being added has cycles,
            // as it's not obvious that the user has to set the *ivars* to nil to
            // avoid leaking memory
            if (prop.type == RLMPropertyTypeObject || prop.type == RLMPropertyTypeArray) {
                ((void(*)(id, SEL, id))objc_msgSend)(obj, prop.setterSel, nil);
            }
        }

        return value;
    }

    id doGetValue(id obj, size_t propIndex, __unsafe_unretained RLMProperty *const prop) {
        // Property value from an NSArray
        if ([obj respondsToSelector:@selector(objectAtIndex:)])
            return propIndex < [obj count] ? [obj objectAtIndex:propIndex] : nil;

        // Property value from an NSDictionary
        if ([obj respondsToSelector:@selector(objectForKey:)])
            return [obj objectForKey:prop.name];

        // Property value from an instance of this object type
        if ([obj isKindOfClass:info.rlmObjectSchema.objectClass]) {
            if (prop.swiftIvar) {
                if (prop.type == RLMPropertyTypeArray) {
                    return static_cast<RLMListBase *>(object_getIvar(obj, prop.swiftIvar))._rlmArray;
                }
                else { // optional
                    return static_cast<RLMOptionalBase *>(object_getIvar(obj, prop.swiftIvar)).underlyingValue;
                }
            }
        }

        // Property value from some object that's KVC-compatible
        return [obj valueForKey:[obj respondsToSelector:prop.getterSel] ? prop.getterName : prop.name];
    }
};
}

namespace realm {
template<>
class NativeAccessor<id, Context*> {
public:
    static id value_for_property(Context* c, id dict, std::string const&, size_t prop_index) {
        return c->value(dict, prop_index);
    }

    static bool dict_has_value_for_key(Context*, id dict, const std::string &prop_name) {
        if ([dict respondsToSelector:@selector(objectForKey:)]) {
            return [dict objectForKey:@(prop_name.c_str())];
        }
        return [dict valueForKey:@(prop_name.c_str())];
    }

    static id dict_value_for_key(Context*, id dict, const std::string &prop_name) {
        return [dict valueForKey:@(prop_name.c_str())];
    }

    static size_t list_size(Context*, id v) { return [v count]; }
    static id list_value_at_index(Context*, id v, size_t index) {
        return [v objectAtIndex:index];
    }

    static bool has_default_value_for_property(Context* c, Realm*, ObjectSchema const&,
                                               std::string const& prop)
    {
        return c->defaultValue(@(prop.c_str()));
    }

    static id default_value_for_property(Context* c, Realm*, ObjectSchema const&,
                                         std::string const& prop)
    {
        return c->defaultValue(@(prop.c_str()));
    }

    static Timestamp to_timestamp(Context*, id v) { return RLMTimestampForNSDate(v); }
    static bool to_bool(Context*, id v) { return [v boolValue]; }
    static double to_double(Context*, id v) { return [v doubleValue]; }
    static float to_float(Context*, id v) { return [v floatValue]; }
    static long long to_long(Context*, id v) { return [v longLongValue]; }
    static BinaryData to_binary(Context*, id v) { return RLMBinaryDataForNSData(v); }
    static StringData to_string(Context*, id v) { return RLMStringDataWithNSString(v); }
    static Mixed to_mixed(Context*, id) { throw std::logic_error("'Any' type is unsupported"); }

    static id from_binary(Context*, BinaryData v) { return RLMBinaryDataToNSData(v); }
    static id from_bool(Context*, bool v) { return @(v); }
    static id from_double(Context*, double v) { return @(v); }
    static id from_float(Context*, float v) { return @(v); }
    static id from_long(Context*, long long v) { return @(v); }
    static id from_string(Context*, StringData v) { return @(v.data()); }
    static id from_timestamp(Context*, Timestamp v) { return RLMTimestampToNSDate(v); }
    static id from_list(Context*, List v) {
        // FIXME: this isn't possible; need parent RLMObject
        return nil;
    }
    static id from_results(Context*, Results v) {
        return nil;
    }
    static id from_object(Context*, Object v) {
        return nil;
    }

    static bool is_null(Context*, id v) { return !v || v == NSNull.null; }
    static id null_value(Context*) { return nil; }

    static size_t to_existing_object_index(Context*, SharedRealm, id &) { return 0; }
    static size_t to_object_index(Context* c, SharedRealm realm, id value, std::string const& object_type, bool update)
    {
        if (auto object = RLMDynamicCast<RLMObjectBase>(value)) {
            if (object->_info && object->_realm->_realm == realm && object->_info->objectSchema->name == object_type) {
                RLMVerifyAttached(object);
                return object->_row.get_index();
            }
        }

        Context subContext{c->realm, c->realm->_info[@(object_type.c_str())]};
        // FIXME: needs to call RLMAddObjectToRealm to promote unmanaged accessors
        return Object::create(&subContext, realm, *realm->schema().find(object_type), value, update).row().get_index();
    }
};
}

void RLMRealmCreateAccessors(RLMSchema *schema) {
    const size_t bufferSize = sizeof("RLM:Managed  ") // includes null terminator
                            + std::numeric_limits<unsigned long long>::digits10
                            + realm::Group::max_table_name_length;

    char className[bufferSize] = "RLM:Managed ";
    char *const start = className + strlen(className);

    for (RLMObjectSchema *objectSchema in schema.objectSchema) {
        if (objectSchema.accessorClass != objectSchema.objectClass) {
            continue;
        }

        static unsigned long long count = 0;
        sprintf(start, "%llu %s", count++, objectSchema.className.UTF8String);
        objectSchema.accessorClass = RLMManagedAccessorClassForObjectClass(objectSchema.objectClass, objectSchema, className);
    }
}

static inline void RLMVerifyRealmRead(__unsafe_unretained RLMRealm *const realm) {
    if (!realm) {
        @throw RLMException(@"Realm must not be nil");
    }
    [realm verifyThread];
}

static inline void RLMVerifyInWriteTransaction(__unsafe_unretained RLMRealm *const realm) {
    RLMVerifyRealmRead(realm);
    // if realm is not writable throw
    if (!realm.inWriteTransaction) {
        @throw RLMException(@"Can only add, remove, or create objects in a Realm in a write transaction - call beginWriteTransaction on an RLMRealm instance first.");
    }
}

void RLMInitializeSwiftAccessorGenerics(__unsafe_unretained RLMObjectBase *const object) {
    if (!object || !object->_row || !object->_objectSchema->_isSwiftClass) {
        return;
    }
    if (![object isKindOfClass:object->_objectSchema.objectClass]) {
        // It can be a different class if it's a dynamic object, and those don't
        // require any init here (and would crash since they don't have the ivars)
        return;
    }

    for (RLMProperty *prop in object->_objectSchema.swiftGenericProperties) {
        if (prop->_type == RLMPropertyTypeArray) {
            RLMArray *array = [[RLMArrayLinkView alloc] initWithParent:object property:prop];
            [object_getIvar(object, prop.swiftIvar) set_rlmArray:array];
        }
        else if (prop.type == RLMPropertyTypeLinkingObjects) {
            id linkingObjects = object_getIvar(object, prop.swiftIvar);
            [linkingObjects setObject:(id)[[RLMWeakObjectHandle alloc] initWithObject:object]];
            [linkingObjects setProperty:prop];
        }
        else {
            RLMOptionalBase *optional = object_getIvar(object, prop.swiftIvar);
            optional.property = prop;
            optional.object = object;
        }
    }
}

void RLMAddObjectToRealm(__unsafe_unretained RLMObjectBase *const object,
                         __unsafe_unretained RLMRealm *const realm,
                         bool createOrUpdate) {
    RLMVerifyInWriteTransaction(realm);

    // verify that object is unmanaged
    if (object.invalidated) {
        @throw RLMException(@"Adding a deleted or invalidated object to a Realm is not permitted");
    }
    if (object->_realm) {
        if (object->_realm == realm) {
            // Adding an object to the Realm it's already manged by is a no-op
            return;
        }
        // for differing realms users must explicitly create the object in the second realm
        @throw RLMException(@"Object is already managed by another Realm");
    }
    if (object->_observationInfo && object->_observationInfo->hasObservers()) {
        @throw RLMException(@"Cannot add an object with observers to a Realm");
    }

    auto& info = realm->_info[object->_objectSchema.className];
    Context c{realm, info};
    object->_row = realm::Object::create(&c, realm->_realm, *info.objectSchema, (id)object, createOrUpdate).row();
    object->_info = &info;
    object->_realm = realm;
    object->_objectSchema = info.rlmObjectSchema;
    object_setClass(object, info.rlmObjectSchema.accessorClass);
}

RLMObjectBase *RLMCreateObjectInRealmWithValue(RLMRealm *realm, NSString *className,
                                               id value, bool createOrUpdate = false) {
    RLMVerifyInWriteTransaction(realm);

    if (createOrUpdate && RLMIsObjectSubclass([value class])) {
        RLMObjectBase *obj = value;
        if (obj->_realm == realm && [obj->_objectSchema.className isEqualToString:className]) {
            // This is a no-op if value is an RLMObject of the same type already backed by the target realm.
            return value;
        }
    }

    auto& info = realm->_info[className];
    Context c{realm, info};
    RLMObjectBase *object = RLMCreateManagedAccessor(info.rlmObjectSchema.accessorClass, realm, &info);
    object->_row = realm::Object::create(&c, realm->_realm, *info.objectSchema, (id)value, createOrUpdate).row();
    RLMInitializeSwiftAccessorGenerics(object);
    return object;
}

void RLMDeleteObjectFromRealm(__unsafe_unretained RLMObjectBase *const object,
                              __unsafe_unretained RLMRealm *const realm) {
    if (realm != object->_realm) {
        @throw RLMException(@"Can only delete an object from the Realm it belongs to.");
    }

    RLMVerifyInWriteTransaction(object->_realm);

    // move last row to row we are deleting
    if (object->_row.is_attached()) {
        RLMTrackDeletions(realm, ^{
            object->_row.get_table()->move_last_over(object->_row.get_index());
        });
    }

    // set realm to nil
    object->_realm = nil;
}

void RLMDeleteAllObjectsFromRealm(RLMRealm *realm) {
    RLMVerifyInWriteTransaction(realm);

    // clear table for each object schema
    for (auto& info : realm->_info) {
        RLMClearTable(info.second);
    }
}

RLMResults *RLMGetObjects(RLMRealm *realm, NSString *objectClassName, NSPredicate *predicate) {
    RLMVerifyRealmRead(realm);

    // create view from table and predicate
    RLMClassInfo& info = realm->_info[objectClassName];
    if (!info.table()) {
        // read-only realms may be missing tables since we can't add any
        // missing ones on init
        return [RLMResults resultsWithObjectInfo:info results:{}];
    }

    if (predicate) {
        realm::Query query = RLMPredicateToQuery(predicate, info.rlmObjectSchema, realm.schema, realm.group);
        return [RLMResults resultsWithObjectInfo:info
                                         results:realm::Results(realm->_realm, std::move(query))];
    }

    return [RLMResults resultsWithObjectInfo:info
                                     results:realm::Results(realm->_realm, *info.table())];
}

id RLMGetObject(RLMRealm *realm, NSString *objectClassName, id key) {
    RLMVerifyRealmRead(realm);

    RLMClassInfo& info = realm->_info[objectClassName];
    auto primaryProperty = info.objectSchema->primary_key_property();
    if (!primaryProperty) {
        @throw RLMException(@"%@ does not have a primary key", objectClassName);
    }

    auto table = info.table();
    if (!table) {
        // read-only realms may be missing tables since we can't add any
        // missing ones on init
        return nil;
    }

    key = RLMCoerceToNil(key);
    if (!key && !primaryProperty->is_nullable) {
        @throw RLMException(@"Invalid null value for non-nullable primary key.");
    }

    size_t row = realm::not_found;
    switch (primaryProperty->type) {
        case PropertyType::String: {
            NSString *string = RLMDynamicCast<NSString>(key);
            if (!key || string) {
                row = table->find_first_string(primaryProperty->table_column, RLMStringDataWithNSString(string));
            } else {
                @throw RLMException(@"Invalid value '%@' of type '%@' for string primary key.", key, [key class]);
            }
            break;
        }
        case PropertyType::Int:
            if (NSNumber *number = RLMDynamicCast<NSNumber>(key)) {
                row = table->find_first_int(primaryProperty->table_column, number.longLongValue);
            } else if (!key) {
                row = table->find_first_null(primaryProperty->table_column);
            } else {
                @throw RLMException(@"Invalid value '%@' of type '%@' for int primary key.", key, [key class]);
            }
            break;
        default:
            REALM_UNREACHABLE();
    }

    if (row == realm::not_found) {
        return nil;
    }

    return RLMCreateObjectAccessor(realm, info, row);
}

RLMObjectBase *RLMCreateObjectAccessor(__unsafe_unretained RLMRealm *const realm,
                                       RLMClassInfo& info,
                                       NSUInteger index) {
    return RLMCreateObjectAccessor(realm, info, (*info.table())[index]);
}

// Create accessor and register with realm
RLMObjectBase *RLMCreateObjectAccessor(__unsafe_unretained RLMRealm *const realm,
                                       RLMClassInfo& info,
                                       realm::RowExpr row) {
    RLMObjectBase *accessor = RLMCreateManagedAccessor(info.rlmObjectSchema.accessorClass, realm, &info);
    accessor->_row = row;
    RLMInitializeSwiftAccessorGenerics(accessor);
    return accessor;
}
