--[[
	Maids handle garbage collection, and move it to the constructor so you
	don't have remember to manually destroy objects. Maids have built-in
	handlers for things like signals and instances, but rely on an object
	to have a :Destroy() or :destroy() method.

	They can be linked such that maids can clean up other maids, or objects
	that utilize maids. This can be done by implementing a destroy method
	that calls the maid to do cleaning, which cleans up that object's fields.

	```lua
	function MyClass.new()
		...
		self._maid = Maid.new()
		...
	end

	function MyClass:destroy()
		self._maid:doCleaning()
	end
	```
]]

local ReplicatedStorage = game:GetService("ReplicatedStorage")
local sharedModules = ReplicatedStorage:WaitForChild("Modules")

local Promise = require(sharedModules.Promise)

local Maid = {}
Maid.__index = Maid

function Maid.new()
	local self = {}
	setmetatable(self, Maid)

	self._given = {}
	return self
end

function Maid.is(object)
	return type(object) == "table" and getmetatable(object) == Maid
end

--[[
	Destroys an objet that was given to the maid previously.
]]
function Maid._destroyObject(object)
	local objectType = typeof(object)
	if objectType == "Instance" then
		object:Destroy()
	elseif objectType == "RBXScriptConnection" then
		if object.Connected then
			object:Disconnect()
		end
	elseif objectType == "function" then
		object()
	elseif objectType == "thread" then
		task.cancel(object)
	elseif objectType == "table" then
		if typeof(object.destroy) == "function" then
			object:destroy()
		elseif typeof(object.Destroy) == "function" then
			object:Destroy()
		elseif Promise.is(object) then
			object:cancel()
		else
			local metaTable = getmetatable(object)
			if metaTable and metaTable.__call then
				metaTable.__call(object)
			else
				-- At this point, it's safe to assume its an array of objects
				-- (pairs must be used in case its a dictionary).
				for _, child in pairs(object) do
					Maid._destroyObject(child)
				end
			end
		end
	end
end

--[[
	Gives the maid a promise.

	This should be used in place of the give function,
	which will not allow the promise to be garbage-collected properly.
]]
function Maid:givePromise(promise)
	if promise:getStatus() ~= Promise.Status.Started then
		return promise
	end

	local newPromise = Promise.new(function(resolve, _, onCancel)
		if onCancel(function()
			promise:cancel()
		end) then
			resolve(promise)
		end
	end)

	self:give(newPromise)
	newPromise:finallyCall(self.clean, self, newPromise)
	return newPromise
end

--[[
	Gives the maid an object to clean up later.
]]
function Maid:give(object)
	table.insert(self._given, object)
	return object
end

--[[
	Destroys a given object that was previously
	given to the maid.
]]
function Maid:clean(object)
	local index = typeof(object) == "number" and object or table.find(self._given, object)

	Maid._destroyObject(self._given[index])
	table.remove(self._given, index)
end

--[[
	Removes an object previously given to the maid
	but does NOT destroy it.
]]
function Maid:remove(object)
	local index = typeof(object) == "number" and object or table.find(self._given, object)
	table.remove(self._given, index)
end

--[[
	Cleans up all objects that were given to the maid.
]]
function Maid:doCleaning()
	for _, object in ipairs(self._given) do
		Maid._destroyObject(object)
	end
end
