package org.lib.sharding.memcached;

import net.spy.memcached.CASMutation;
import net.spy.memcached.CASMutator;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.Serializable;
import java.util.concurrent.Future;

import static java.lang.Boolean.TRUE;

@Singleton
public class MemcachedClient {
	private static final Logger log = LoggerFactory.getLogger(MemcachedClient.class);

	private final net.spy.memcached.MemcachedClient client;

	@Inject
	public MemcachedClient(net.spy.memcached.MemcachedClient client) {
		this.client = client;
	}

	public <T> void set(final String key, final int expire, final T value) {
		log.debug("Set value({}) with key({})", value, key);
		modifyCache(
			new Provider<Future<Boolean>>() {
				@Override
				public Future<Boolean> get() {
					return client.set(key, expire, value, new SerializingTranscoder());
				}
			},
			false
		);
	}

	public <T> void set(final String key, final int expire, final T value, final Transcoder<T> transcoder) {
		log.debug("Set value({}) with key({})", value, key);
		modifyCache(
			new Provider<Future<Boolean>>() {
				@Override
				public Future<Boolean> get() {
					return client.set(key, expire, value, transcoder);
				}
			},
			false
		);
	}

	@SuppressWarnings("unchecked")
	public <T> T cas(final String key, final int expire, final T initialValue, final CASMutation<T> mutation) {
		log.debug("Cas value({}) with key({})", initialValue, key);
		CASMutator<T> mutator = new CASMutator<T>(client, (Transcoder<T>) client.getTranscoder());
		try {
			T stored = mutator.cas(key, initialValue, expire, mutation);
			log.debug("Stored: {}", stored);
			return stored;
		} catch (Exception e) {
			log.error(String.format("Can't cas value with key(%s)", key), e);
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T get(final String key) {
		T value = (T) client.get(key, new SerializingTranscoder());
		log.debug("Get value({}) with key({})", value, key);
		return value;
	}

	public <T> T get(final String key, final Transcoder<T> transcoder) {
		T value = client.get(key, transcoder);
		log.debug("Get value({}) with key({})", value, key);
		return value;
	}

	public void delete(final String key) {
		modifyCache(
			new Provider<Future<Boolean>>() {
				@Override
				public Future<Boolean> get() {
					return client.delete(key);
				}
			},
			true
		);
	}

	private void modifyCache(Provider<Future<Boolean>> modifyFunction, boolean discardResult) {
		try {
			Future<Boolean> result = modifyFunction.get();
			Boolean realResult = result.get();
			if (!discardResult && !TRUE.equals(realResult)) {
				log.error("Can't modify value in the memcached");
				throw new RuntimeException("Can't modify value in the memcached!");
			}
		} catch (Exception e) {
			log.error("Can't modify value in the memcached", e);
			throw new RuntimeException(e);
		}
	}
}
