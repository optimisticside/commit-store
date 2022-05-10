local LOCK_DEATH_THRESHOLD = 20
local UPDATE_INTERVAL = 60 * 60
local QUEUE_MAX_LENGTH = 20
local CHECK_INTERVAL = 5
local ACK_INTERVAL = 30

local MemoryStoreService = game:GetService("MemoryStoreService")
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local DataStoreService = game:GetService("DataStoreService")
local RunService = game:GetService("RunService")

-- Update the paths to these modules if they do not match the location
-- in your game.
local Promise = require(ReplicatedStorage.Promise)
local Maid = require(ReplicatedStorage.Maid)

local function defaultDifferentiator(previous, current)
	local changes = {}

	for index, value in pairs(current) do
		if previous[index] ~= value then
			if typeof(previous[index]) == "table" and typeof(value) == "table" then
				value = defaultDifferentiator(previous[index], value)
			end

			changes[index] = value
		end
	end

	-- Go through the table and set any values that were removed to be `{}`.
	-- This works because if a table is empty we can set it to `nil` anyway.
	for index in pairs(previous) do
		if not current[index] then
			changes[index] = {}
		end
	end

	return changes
end

local function defaultIntegrator(current, changes)
	for index, change in pairs(changes) do
		if typeof(change) == "table" then
			if not #change then
				change = nil
			elseif typeof(current[index]) == "table" then
				change = defaultIntegrator(current[index], change)
			end
		end

		current[index] = change
	end
end

local DataStore = {}
DataStore.__index = DataStore

function DataStore.new(name, serverId, integrator, differentiator)
	local self = {}
	setmetatable(self, DataStore)

	self._name = name
	self._maid = Maid.new()
	self._serverId = serverId or ("%d:%s"):format(game.PlaceId, game.JobId)
	self._dataStore = DataStoreService:GetDataStore(name)
	self._keyData = MemoryStoreService:GetSortedMap(name)
	self._commitQueues = {}
	self._ownedKeys = {}

	self._differentiator = differentiator or defaultDifferentiator
	self._integrator = integrator or defaultIntegrator

	self._lastCheck = 0
	-- TODO: Can we spawn a thread (it can be canceled by the maid through
	-- `task.cancel`) instead of this run-service collection that we know runs
	-- way too often.
	self._maid:give(RunService.Stepped:Connect(function()
		if os.clock() - self._lastCheck >= CHECK_INTERVAL then
			self:_checkOwnedKeysAsync()
		end
	end))

	return self
end

function DataStore.is(object)
	return type(object) == "table" and getmetatable(object) == DataStore
end

function DataStore:destroy()
	self._maid:doCleaning()
end

--[[
	Gets the queue of commits of the given key.
]]
function DataStore:_getCommitQueue(key)
	local cached = self._commitQueues[key]
	if cached then
		return cached
	end

	local queueKey = ("%s:%s"):format(self._name, key)
	local saved = MemoryStoreService:GetQueue(queueKey)

	self._commitQueues[key] = saved
	return saved
end

--[[
	Creates the key-data for a key in the data-store.
]]
function DataStore:_createKeyData()
	return {
		owner = self._serverId,
		lastAck = os.time(),
		lastSave = 0,
	}
end

--[[
	Saves data to the data-store. This data must be manually calculated through
	an integrator. Note that only the diffs must be calculated, not the actual
	data.
]]
function DataStore:_syncToDataStoreAsync(key, diff)
	local commitQueue = self._getCommitQueue(key)

	return Promise.try(commitQueue.ReadAsync, commitQueue, QUEUE_MAX_LENGTH):andThen(function(commits)
		return Promise.try(self._dataStore.UpdateAsync, self._dataStore, key, function(latest)
			if diff ~= nil then
				return self._integrator(latest, diff)
			end

			-- Default to getting the latest data from
			-- `DataStore::getLatestAsync`. Note that this will not yield since
			-- we've provided everything the function needs already.
			return self:getLatestAsync(key, latest, commits):await()
		end)
	end)
end

--[[
	Checks if any commits need to be saved. This is automatically called every
	`CHECK_INTERVAL` by a connection to `RunService.Stepped`.
]]
function DataStore:_checkOwnedKeysAsync()
	local promises = {}

	for _, key in ipairs(self._ownedKeys) do
		table.insert(promises, self:syncCommitsAsync(key))
	end

	return Promise.all(promises)
end

--[[
	Re-agknowledges that we still own the keys that we own. This system is in
	place so that if a server goes down, another server can take over
	ownership.
]]
function DataStore:_agknowledgeKeyAsync(key)
	return Promise.try(self._keyData.UpdateAsync, self._keyData, key, function(keyData)
		if not keyData then
			-- `DataStore::_createKeyData` already sets the last
			-- agknowledgement to the current time.
			return self:_createKeyData()
		end

		if keyData.owner ~= self._serverId then
			local index = table.find(self._ownedKeys, key)
			if index then
				table.remove(self._ownedKeys, index)
			end

			return nil
		end

		if keyData.owner == self._serverId and os.time() - keyData.lastAck >= ACK_INTERVAL then
			keyData.lastAck = os.time()
			return keyData
		end
	end)
end

--[[
	Computes the latest version from the commits and the data-store's original
	data and updates the data-store.
]]
function DataStore:syncCommitsAsync(key)
	-- We lose atomicity here, since we cannot wrap everything in an
	-- update-async call, though this is not important here.
	return Promise.try(self._keyData.GetAsync, self._keyData, key):andThen(function(keyData)
		if not keyData then
			return self:_createKeyData()
		end

		if keyData.owner == self._serverId and os.time - keyData.lastSave >= UPDATE_INTERVAL then
			return self:_syncToDataStoreAsync(key):andThen(function()
				-- Key-data may have changed since we last so we retrieve
				-- it again through a call to update-async.
				return Promise.try(self._keyData.UpdateAsync, self._keyData, key, function(newKeyData)
					newKeyData.lastSave = os.time()
					return newKeyData
				end)
			end)
		end
	end)
end

--[[
	Tries to steal a lock, if the server has not acknowledged ownership
	recently enough.
]]
function DataStore:_tryStealLockAsync(key)
	return Promise.try(self._keyData.UpdateAsync, self._keyData, key, function(keyData)
		if not keyData then
			return self:_createKeyData()
		end

		-- We use a binary spin-lock for acquiring key ownership, since using a
		-- queue would be too much work. Actually, we could use a number system
		-- where each server keeps a number that represents their position in
		-- the queue. TODO: ^
		if keyData.owner ~= self._serverId and os.time() - keyData.lastAck >= LOCK_DEATH_THRESHOLD then
			keyData.owner = self._serverId
			keyData.lastAck = os.time()

			table.insert(self._ownedKeys, key)
			return keyData
		end
	end)
end

--[[
	Retrieves the commits made by other servers, as well as what is in the
	data-store to compute the most up-to-date value.
]]
function DataStore:getLatestAsync(key, givenLatest, givenCommits)
	-- This ugly chain of promises is so that we can manually provide a list
	-- of promises to the function, which is necessary because you cannot yield
	-- inside of callbacks.
	return Promise.new(function()
		if givenCommits then
			return givenCommits
		end

		local commitQueue = self._getCommitQueue(key)
		return Promise.try(commitQueue.ReadAsync, commitQueue, QUEUE_MAX_LENGTH)
	end):andThen(function(commits)
		return Promise.new(function()
			return givenLatest or Promise.try(self._dataStore.GetAsync, self._dataStore, key)
		end):andThen(function(latest)
			for _, commit in ipairs(commits) do
				latest = self._integrator(latest, commit.diff)
			end

			return latest
		end)
	end)
end

--[[
	Creates a commit based on the deltas provided (these must be computed
	through the differentiator if needed) and returns a promise that will
	be resolved once the commit has been made.

	```lua
	-- You don't always need to call commitAsync if you know what is
	-- being changed in the data.
	dataStore:commitDiffAsync(key, { updated = true })
	```
]]
function DataStore:commitDiffAsync(key, diff)
	local commitQueue = self:_getCommitQueue(key)
	-- Each commit contains the diff, as in, the new data that was added. It
	-- also contains when the data was added and by who. This metadata helps
	-- keep track of how old a commit is and by when it must be saved.
	return Promise.try(commitQueue.AddAsync, commitQueue, {
		author = self._serverId,
		time = os.time(),
		diff = diff,
	}):andThen(function()
		-- This might a bad descision overall...
		return self:_tryStealLockAsync()
	end)
end

--[[
	Commits data to the data-store and returns a promise that will be resolved
	once the commit has been made.
]]
function DataStore:commitAsync(key, data)
	return self:getLatestAsync(key):andThen(function(latest)
		local diff = self._differentiator(latest, data)
		return self:commitDiffAsync(key, diff)
	end)
end

return DataStore
