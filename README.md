# Commit Store
Commit-store is a wrapper for Roblox's data-stores and creates a system of commits, where each commit store what changes need to be made to the previous version of the data to get a newer version of it.

Commit-store uses a system of an integrator and a differentiator, where the integrator can integrate the changes in a commit to older data and the differentiator can differentiate between two version of the data to get the changes that were made to it.

# Usage
Using the library is very simple, you can create a `CommitStore` object by doing `CommitStore.new(dataStoreName)` and providing the function with the name of the data-store.

Once you have a `CommitStore` object you need to know about three main functions that you can use: `getLatestAsync`, `commitAsync`, and `commitDiffAsync`. Note that all these functions return a [promise](https://eryn.io/roblox-lua-promise/)

`getLatestAsync` gets the latest data in the data-store, and is similar to `GetAsync` in traditional data-stores.

`commitAsync` will commit changes to the data based on the latest version of it. It will internally compute what changes you made to the data since the last commit.

`commitDiffAsync` will just commit the changes made the data based on what you provided. This method is useful for when you know what data you are changing. For example, you might do `commitStore:commitDiffAsync(key, { coins = 100 })` to set the value of `coins` to `100`.
