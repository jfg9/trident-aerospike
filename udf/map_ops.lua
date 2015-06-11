-- ======================================================================
-- Map Operations
-- ======================================================================
-- These UDFs are for maintaining a map of keys to values in Aerospike.
-- Each function operates on a map in a specified bin.
-- This enables us to partially read or write to map types without
-- reading the whole map onto the client.
--
-- Operations are:
-- 1. Read value for a given key
-- 2. Write value for a given key, overwriting any existing value
-- 3. Add value to an existing value for a given key (assumes numeric type)
--
-- Author: joshua.forman-gornall@adform.com
-- ======================================================================

function read_val( rec, binName, mapKey )
	if( not aerospike:exists(rec) ) then
		return nil;
	end
	local binVal = rec[binName];
	if ( binVal == nil ) then
		return nil;
	end
	return binVal[mapKey];
end

function write_val( rec, binName, mapKey, newValue )
	if( not aerospike:exists(rec) ) then
		rc = aerospike:create(rec);
		if( rc == nil ) then
			warn("[ERROR] Problem creating record");
			error("ERROR creating record");
		end
		local m = map();
		m[mapKey] = newValue;
		rec[binName] = m;
	else
		local binVal = rec[binName];
		if( binVal == nil ) then
			local m = map();
			m[mapKey] = newValue;
			rec[binName] = m;
		else
			binVal[mapKey] = newValue;
			rec[binName] = binVal;
		end
	end
	rc = aerospike:update(rec);
	if( rc ~= nil and rc ~= 0 ) then
		warn("[ERROR] Record update failed: rc(%s)", tostring(rc));
		error("ERROR updating the record");
	end
    return 0;
end

function add_count( rec, binName, mapKey, valueToAdd )
	if( not aerospike:exists(rec) ) then
		rc = aerospike:create(rec);
		if( rc == nil ) then
			warn("[ERROR] Problem creating record");
			error("ERROR creating record");
		end
		local m = map();
		m[mapKey] = valueToAdd;
		rec[binName] = m;
	else
		local binVal = rec[binName];
		if( binVal == nil ) then
			local m = map();
			m[mapKey] = valueToAdd;
			rec[binName] = m;
		else
			if( binVal[mapKey] == nil ) then
				binVal[mapKey] = valueToAdd;
			else
				binVal[mapKey] = binVal[mapKey] + valueToAdd;
			end
			rec[binName] = binVal;
		end
	end
	rc = aerospike:update(rec);
	trace("<CHECK> Check Update Result(%s)", tostring(rc));
	if( rc ~= nil and rc ~= 0 ) then
		warn("[ERROR] Record update failed: rc(%s)", tostring(rc));
		error("ERROR updating the record");
	end
    return 0;
end