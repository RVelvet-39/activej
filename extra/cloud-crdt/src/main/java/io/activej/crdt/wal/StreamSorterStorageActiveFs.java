/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.crdt.wal;

import io.activej.common.MemSize;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.processor.StreamSorterStorage;
import io.activej.fs.ActiveFs;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.activej.datastream.csp.ChannelSerializer.DEFAULT_INITIAL_BUFFER_SIZE;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;


// TODO: abstract out with StreamSorterStorageImpl
final class StreamSorterStorageActiveFs<T> implements StreamSorterStorage<T> {
	private static final AtomicInteger PARTITION = new AtomicInteger();

	private final ActiveFs fs;
	private final BinarySerializer<T> serializer;
	private final FrameFormat frameFormat;

	private final MemSize readBlockSize = DEFAULT_INITIAL_BUFFER_SIZE;
	private final MemSize writeBlockSize = MemSize.kilobytes(256);

	StreamSorterStorageActiveFs(ActiveFs fs, BinarySerializer<T> serializer, FrameFormat frameFormat) {
		this.fs = fs;
		this.serializer = serializer;
		this.frameFormat = frameFormat;
	}

	private String partitionFilename(int i) {
		return format("%d", i);
	}

	@Override
	public Promise<Integer> newPartitionId() {
		return Promise.of(PARTITION.incrementAndGet());
	}

	@Override
	public Promise<StreamConsumer<T>> write(int partition) {
		return Promise.of(StreamConsumer.ofSupplier(
				supplier -> supplier
						.transformWith(ChannelSerializer.create(serializer)
								.withInitialBufferSize(readBlockSize))
						.transformWith(ChannelByteChunker.create(writeBlockSize.map(bytes -> bytes / 2), writeBlockSize))
						.transformWith(ChannelFrameEncoder.create(frameFormat))
						.transformWith(ChannelByteChunker.create(writeBlockSize.map(bytes -> bytes / 2), writeBlockSize))
						.streamTo(fs.upload(partitionFilename(partition)))));
	}

	@Override
	public Promise<StreamSupplier<T>> read(int partition) {
		return fs.download(partitionFilename(partition))
				.map(file -> file
						.transformWith(ChannelFrameDecoder.create(frameFormat))
						.transformWith(ChannelDeserializer.create(serializer)));
	}

	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		return fs.deleteAll(partitionsToDelete.stream()
				.map(this::partitionFilename)
				.collect(toSet()));
	}
}
