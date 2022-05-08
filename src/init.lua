local LOCK_DEATH_THRESHOLD = 20
local CACHE_LIFETIME = 60 * 60
local QUEUE_MAX_LENGTH = 20
local CHECK_INTERVAL = 5

local MemoryStoreService = game:GetService("MemoryStoreService")
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local DataStoreService = game:GetService("DataStoreService")
local RunService = game:GetService("RunService")

-- Update the paths to these modules if they do not match the location
-- in your game.
local Promise = require(ReplicatedStorage.Promise)
local Signal = require(ReplicatedStorage.Signal)
local Maid = require(ReplicatedStorage.Maid)

local function defaultDifferentiator(previous, current)
	-- TODO: (this applies to the integrator too) How do we represent data that
	-- has been deleted? We cannot just set it to `nil`.
end

local function defaultIntegrator(current, changes)
	for index, change in pairs(changes) do
		if typeof(current[index]) == "table" and typeof(change) == "table" then
			change = defaultIntegrator(current[index], change)
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
	self._serverId = serverId or ("%s:%s"):format(game.PlaceId, game.JobId)
	self._keyLocks = MemoryStoreService:GetSortedMap(name)
	self._dataStore = DataStoreService:GetDataStore(name)
	self._commitQueues = {}

	self._differentiator = differentiator or defaultDifferentiator
	self._integrator = integrator or defaultIntegrator

	self._lastCheck = 0
	-- TODO: Can we spawn a thread (it can be canceled by the maid through
	-- `task.cancel`) instead of this run-service collection that we know runs
	-- way too often.
	self._maid:give(RunService.Stepped:Connect(function()
		if os.clock() - self._lastCheck >= CHECK_INTERVAL then

		end
	end))

	return self
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
	Saves data to the data-store. This data must be manually calculated through
	an integrator. Note that only the diffs must be calculated, not the actual
	data.
]]
function DataStore:_syncToDataStoreAsync(key, diff)
	return Promise.try(self._dataStore.UpdateAsync, self._dataStore, function(latest)
		latest = self._integrator(latest, diff)
		return self:getLatestAsync(key, latest)
	end)
end

--[[
	Retrieves the commits made by other servers, as well as what is in the
	data-store to compute the most up-to-date value.
]]
function DataStore:getLatestAsync(key, latest)
	local commitQueue = self._getCommitQueue(key)

	return Promise.try(commitQueue.ReadAsync, commitQueue, QUEUE_MAX_LENGTH):andThen(function(commits)
		latest = latest or Promise.try(self._dataStore.GetAsync, self._dataStore, key):await()

		for _, commit in ipairs(commits) do
			latest = self._integrator(latest, commit.diff)
		end

		return latest
	end)
end

--[[
	Creates a commit based on the deltas provided (these must be computed
	through the differentiator if needed) and returns a promise that will
	be resolved once the commit has been made.

	```lua
	-- You don't always need to use the differentiator if you know what is
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
	})
end

--[[
	Commits data to the data-store and returns a promise that will be resolved
	once the commit has been made.
]]
function DataStore:commitAsync(key, data)
	return self:getLatestAsync(key):andThen(function(latest)
		local diff = self._differentiator(latest, data)
		return self:commitDiffAsync(key, diff):await()
	end)
end

return DataStore