package org.lib.sharding.repository.memcached;

public class SimpleNodeRepository extends BaseNodeRepository {
	private final String key;

	public SimpleNodeRepository(String key) {
		this.key = key;
	}

	@Override
	protected String getNodeCollectionKey() {
		return key;
	}
}
