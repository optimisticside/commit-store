type Integrator<T> = (current: T, diff: Partial<T>) => T;
type Differentiator<T> = (previous: T, current: T) => Partial<T>;

export default interface CommitStore<T> {
	new(
		name: string,
		serverId?: string,
		integrator?: Integrator<T>,
		differentiator?: Differentiator<T>
	): CommitStore<T>;

	/**
	 * Computes the latest version from the commits and the data-store's
	 * original data and updates the data-store.
	 */
	syncCommitsAsync(key: string): Promise<void>;

	/**
	 * Retrieves the commits made by other servers, as well as what is in the
	 * data-store to compute the most up-to-date value.
	 */
	getLatestAsync(key: string): Promise<T>;

	/**
	 * Creates a commit based on the deltas provided (these must be computed
	 * through the differentiator if needed) and returns a promise that will
	 * be resolved once the commit has been made.
	 */
	commitDiffAsync(key: string, diff: Partial<T>): Promise<void>;

	/**
	 * Commits data to the data-store and returns a promise that will be
	 * resolved once the commit has been made. 
	 */
	commitAsync(key: string, value: T): Promise<void>;
}
