--[[
	Commit-based wrapper to Roblox's DataStoreService. Stores commits in
	a memory-store and assigns a server to be the owner of that key, meaning it
	is the one that will update the data-store.

	Ownership must be acknowledged every now and then, or another server will
	take ownership from the server.
]]

local EXPIRATION_TIME = 60 * 60 * 24 * 15
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

	if not previous or type(previous) ~= "table" then
		return current
	end

	for index, value in pairs(current) do
		if previous[index] ~= value then
			if type(previous[index]) == "table" and type(value) == "table" then
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
	current = table.clone(current or {})

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

local CommitStore = {}
CommitStore.__index = CommitStore

function CommitStore.new(name, serverId, integrator, differentiator)
	local self = {}
	setmetatable(self, CommitStore)

	-- The game's job-id is just an empty string if we're running inside
	-- of studio.
	if RunService:IsStudio() and not serverId then
		error("Argument 2 required if in studio")
	end

	self._name = name
	self._maid = Maid.new()
	self._serverId = serverId or ("%d:%s"):format(game.PlaceId, game.JobId)
	self._dataStore = DataStoreService:GetDataStore(name)
	self._keyData = MemoryStoreService:GetSortedMap(name)
	self._commitQueues = {}
	self._ownedKeys = {}

	self._differentiator = differentiator or defaultDifferentiator
	self._integrator = integrator or defaultIntegrator

	self._maid:give(task.spawn(function()
		while true do
			self:_checkOwnedKeysAsync():await()
			task.wait(CHECK_INTERVAL)
		end
	end))

	return self
end

function CommitStore.is(object)
	return type(object) == "table" and getmetatable(object) == CommitStore
end

function CommitStore:destroy()
	self._maid:doCleaning()
end

--[[
	Gets the queue of commits of the given key.
]]
function CommitStore:_getCommitQueue(key)
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
function CommitStore:_createKeyData(currentTime)
	return {
		owner = self._serverId,
		lastAck = currentTime or os.time(),
		lastSave = 0,
	}
end

--[[
	Saves data to the data-store. This data must be manually calculated through
	an integrator. Note that only the diffs must be calculated, not the actual
	data.
]]
function CommitStore:_syncToDataStoreAsync(key, diff)
	local commitQueue = self:_getCommitQueue(key)

	return Promise.try(commitQueue.ReadAsync, commitQueue, QUEUE_MAX_LENGTH, false, 5):andThen(function(commits)
		return Promise.try(self._dataStore.UpdateAsync, self._dataStore, key, function(latest)
			if diff ~= nil then
				return self._integrator(latest, diff)
			end

			-- Default to getting the latest data from
			-- `CommitStore::getLatestAsync`. Note that this will not yield
			-- since we've provided everything the function needs already.
			return self:getLatestAsync(key, latest, commits):await()
		end)
	end)
end

--[[
	Checks if any commits need to be saved. This is automatically called every
	`CHECK_INTERVAL` by a connection to `RunService.Stepped`.
]]
function CommitStore:_checkOwnedKeysAsync()
	local promises = {}

	for _, key in ipairs(self._ownedKeys) do
		table.insert(promises, self:syncCommitsAsync(key))
	end

	return Promise.all(promises)
end

--[[
	Re-acknowledges that we still own the keys that we own. This system is in
	place so that if a server goes down, another server can take over
	ownership.
]]
function CommitStore:_acknowledgeKeyAsync(key)
	local currentTime = os.time()

	return Promise.try(self._keyData.UpdateAsync, self._keyData, key, function(keyData)
		if not keyData then
			-- `CommitStore::_createKeyData` already sets the last
			-- acknowledgement to the current time.
			return self:_createKeyData(currentTime)
		end

		if keyData.owner ~= self._serverId then
			local index = table.find(self._ownedKeys, key)
			if index then
				table.remove(self._ownedKeys, index)
			end

			return nil
		end

		if keyData.owner == self._serverId and currentTime - keyData.lastAck >= ACK_INTERVAL then
			keyData.lastAck = currentTime
			return keyData
		end

		return nil
	end, EXPIRATION_TIME)
end

--[[
	Computes the latest version from the commits and the data-store's original
	data and updates the data-store.
]]
function CommitStore:syncCommitsAsync(key)
	-- We initially lose atomicity here, since we cannot wrap everything in an
	-- update-async call, though this is not important here.
	return Promise.try(self._keyData.GetAsync, self._keyData, key):andThen(function(keyData)
		if not keyData then
			return self:_createKeyData()
		end

		if keyData.owner == self._serverId and os.time() - keyData.lastSave >= UPDATE_INTERVAL then
			return self:_syncToDataStoreAsync(key):andThen(function()
				local currentTime = os.time()

				-- Key-data may have changed since we last so we retrieve
				-- it again through a call to update-async to preserve
				-- atomicity.
				-- TODO: This becomes two get-requests which is not optimal.
				return Promise.try(self._keyData.UpdateAsync, self._keyData, key, function(newKeyData)
					newKeyData.lastSave = currentTime
					return newKeyData
				end, EXPIRATION_TIME)
			end)
		end

		return nil
	end)
end

--[[
	Tries to steal a lock, if the server has not acknowledged ownership
	recently enough.
]]
function CommitStore:_tryStealLockAsync(key)
	local currentTime = os.time()

	return Promise.try(self._keyData.UpdateAsync, self._keyData, key, function(keyData)
		if not keyData then
			return self:_createKeyData()
		end

		-- We use a binary spin-lock for acquiring key ownership, since using a
		-- queue would be too much work. Actually, we could use a number system
		-- where each server keeps a number that represents their position in
		-- the queue. TODO: ^
		if keyData.owner ~= self._serverId and currentTime - keyData.lastAck >= LOCK_DEATH_THRESHOLD then
			keyData.owner = self._serverId
			keyData.lastAck = currentTime

			table.insert(self._ownedKeys, key)
			return keyData
		end

		return nil
	end, EXPIRATION_TIME)
end

--[[
	Retrieves the commits made by other servers, as well as what is in the
	data-store to compute the most up-to-date value.
]]
function CommitStore:getLatestAsync(key, givenLatest, givenCommits)
	-- This ugly chain of promises is so that we can manually provide a list
	-- of promises to the function, which is necessary because you cannot yield
	-- inside of callbacks.
	return Promise.new(function(resolve)
		if givenCommits then
			resolve(givenCommits)
			return
		end

		local commitQueue = self:_getCommitQueue(key)
		resolve(Promise.try(commitQueue.ReadAsync, commitQueue, QUEUE_MAX_LENGTH, false, 5))
	end):andThen(function(commits)
		return Promise.new(function(resolve)
			resolve(givenLatest or Promise.try(self._dataStore.GetAsync, self._dataStore, key))
		end):andThen(function(latest)
			latest = latest or {}

			for _, commit in ipairs(commits or {}) do
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
	commitStore:commitDiffAsync(key, { updated = true })
	```
]]
function CommitStore:commitDiffAsync(key, diff)
	local commitQueue = self:_getCommitQueue(key)
	-- Each commit contains the diff, as in, the new data that was added. It
	-- also contains when the data was added and by who. This metadata helps
	-- keep track of how old a commit is and by when it must be saved.
	return Promise.try(commitQueue.AddAsync, commitQueue, {
		author = self._serverId,
		time = os.time(),
		diff = diff,
	}, EXPIRATION_TIME):andThen(function()
		-- This might a bad descision overall...
		return self:_tryStealLockAsync(key)
	end)
end

--[[
	Commits data to the data-store and returns a promise that will be resolved
	once the commit has been made.
]]
function CommitStore:commitAsync(key, data)
	return self:getLatestAsync(key):andThen(function(latest)
		local diff = self._differentiator(latest, data)
		return self:commitDiffAsync(key, diff)
	end)
end

return CommitStore
