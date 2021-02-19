package io.activej.crdt.wal;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.function.AsyncSuppliers;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelConsumers.ForwardingChannelConsumer;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers;
import io.activej.datastream.processor.StreamSorter;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.async.process.AsyncExecutors.sequential;
import static io.activej.fs.ActiveFsAdapters.subdirectory;
import static java.util.Comparator.naturalOrder;

public class FileWriteAheadLog<K extends Comparable<K>, S> implements WriteAheadLog<K, S>, EventloopService {
	public static final String EXT = ".wal";
	public static final FrameFormat FRAME_FORMAT = LZ4FrameFormat.create();

	private static final int SORT_ITEMS_IN_MEMORY = 100_000;
	private static final int INITIAL_BUF_SIZE = 128;

	private final Eventloop eventloop;
	private final ActiveFs fs;
	private final CrdtFunction<S> function;
	private final CrdtDataSerializer<K, S> serializer;
	private final CrdtStorage<K, S> storage;

	private final Function<CrdtData<K, S>, K> keyFn = CrdtData::getKey;

	private final AsyncSupplier<Void> flush = AsyncSuppliers.reuse(this::doFlush);

	private WalConsumer consumer;
	private boolean stopping;
	private int bufSize = INITIAL_BUF_SIZE;

	public FileWriteAheadLog(
			Eventloop eventloop,
			ActiveFs fs,
			CrdtFunction<S> function,
			CrdtDataSerializer<K, S> serializer,
			CrdtStorage<K, S> storage
	) {
		this.eventloop = eventloop;
		this.fs = fs;
		this.function = function;
		this.serializer = serializer;
		this.storage = storage;
	}

	@Override
	public Promise<Void> put(K key, S value) {
		return consumer.accept(new CrdtData<>(key, value));
	}

	@Override
	public Promise<Void> flush() {
		return flush.get();
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<?> start() {
		return fs.list("*")
				.then(files -> {
					List<String> toBeFlushed = files.keySet().stream()
							.filter(filename -> filename.endsWith(EXT))
							.sorted()
							.collect(Collectors.toList());

					if (toBeFlushed.isEmpty()) return Promise.complete();

					fs.download(toBeFlushed.get(0))
							.then(supplier -> supplier
									.transformWith(ChannelDeserializer.create(serializer))
									.toList());


					return Promise.complete();
				});
	}

	@Override
	public @NotNull Promise<?> stop() {
		stopping = true;
		return flush();
	}

	private WalConsumer createConsumer() {
		String filename = eventloop.currentTimeMillis() + EXT;
		ChannelConsumer<CrdtData<K,S>> consumer = ChannelConsumer.ofPromise(fs.upload(filename))
				.map(this::doEncode)
				.withExecutor(sequential());
		return new WalConsumer(consumer, filename);
	}

	private Promise<Void> doFlush() {
		assert this.consumer != null;

		WalConsumer finishedConsumer = this.consumer;
		ActiveFs sortFs = subdirectory(fs, "sort_" + finishedConsumer.getFilename());
		this.consumer = stopping ? null : createConsumer();

		return finishedConsumer.acceptEndOfStream()
				.then(() -> fs.download(finishedConsumer.getFilename()))
				.then(supplier -> {
					StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> reducer = StreamReducer.create();

					supplier.transformWith(ChannelDeserializer.create(serializer))
							.streamTo(reducer.newInput(CrdtData::getKey, new WalReducer()));

					return reducer.getOutput()
							.transformWith(sorter(sortFs))
							.streamTo(storage.upload());
				});
	}

	private StreamSorter<K, CrdtData<K, S>> sorter(ActiveFs sortFs) {
		StreamSorterStorageActiveFs<CrdtData<K, S>> sorterStorage = new StreamSorterStorageActiveFs<>(sortFs, serializer, FRAME_FORMAT);
		return StreamSorter.create(sorterStorage, keyFn, naturalOrder(), false, SORT_ITEMS_IN_MEMORY);
	}

	private ByteBuf doEncode(CrdtData<K, S> data) {
		while (true) {
			ByteBuf buf = ByteBufPool.allocate(bufSize);
			try {
				buf.tail(serializer.encode(buf.array(), buf.head(), data));
				return buf;
			} catch (ArrayIndexOutOfBoundsException e) {
				bufSize <<= 1;
			}
		}
	}

	private final class WalReducer implements StreamReducers.Reducer<K, CrdtData<K, S>, CrdtData<K, S>, CrdtData<K, S>> {

		@Override
		public CrdtData<K, S> onFirstItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> firstValue) {
			return firstValue;
		}

		@Override
		public CrdtData<K, S> onNextItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> nextValue, CrdtData<K, S> accumulator) {
			return new CrdtData<>(key, function.merge(accumulator.getState(), nextValue.getState()));
		}

		@Override
		public void onComplete(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> accumulator) {
			stream.accept(accumulator);
		}
	}

	private final class WalConsumer extends ForwardingChannelConsumer<CrdtData<K, S>> {
		private final String filename;

		private WalConsumer(ChannelConsumer<CrdtData<K, S>> consumer, String filename) {
			super(consumer);
			this.filename = filename;
		}

		public String getFilename() {
			return filename;
		}
	}
}
