using System.Threading.Channels;

namespace FastAndFurious.Core
{
    public sealed class Processor<T>
    {
        private readonly int _numberOfConcurrentProcessors;
        private readonly Channel<T> _channel;

        private Task? _writer;
        private Task[]? _readers;

        public Processor(int numberOfConcurrentProcessors)
        {
            _numberOfConcurrentProcessors = numberOfConcurrentProcessors;

            var options = new BoundedChannelOptions(_numberOfConcurrentProcessors)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = true
            };

            _channel = Channel.CreateBounded<T>(options);
        }

        public Processor<T> ReadFrom(IEnumerable<T> source, CancellationToken cancellationToken = default)
        {
            _writer = Task.Run(
                async () =>
                {
                    var enumerator = source.GetEnumerator();

                    while (enumerator.MoveNext() && await _channel.Writer.WaitToWriteAsync(cancellationToken))
                    {
                        await _channel.Writer.WriteAsync(enumerator.Current, cancellationToken);
                    }

                    _channel.Writer.Complete();
                },
                cancellationToken);

            return this;
        }

        public Processor<T> ReadFrom(Func<IEnumerable<T>> source, CancellationToken cancellationToken = default)
        {
            _writer = Task.Run(
                async () =>
                {
                    var enumerator = source.Invoke().GetEnumerator();
                    while (enumerator.MoveNext() && await _channel.Writer.WaitToWriteAsync(cancellationToken))
                    {
                        await _channel.Writer.WriteAsync(enumerator.Current, cancellationToken);
                    }

                    _channel.Writer.Complete();
                },
                cancellationToken);


            return this;
        }

        public Processor<T> WriteTo<TResult>(Func<T, Task> expression, CancellationToken cancellationToken = default)
        {
            _readers = Enumerable.Range(0, _numberOfConcurrentProcessors).Select(_ =>
            {
                return Task.Run(
                    async () =>
                    {
                        while (await _channel.Reader.WaitToReadAsync(cancellationToken))
                        {
                            while (_channel.Reader.TryRead(out var item))
                            {
                                await expression(item);
                            }
                        }
                    }, cancellationToken);
            }).ToArray();

            return this;
        }

        public async Task RunAsync()
        {
            ArgumentNullException.ThrowIfNull(_readers);
            ArgumentNullException.ThrowIfNull(_writer);

            await Task.WhenAll(_readers.Append(_writer));
        }
    }
}
