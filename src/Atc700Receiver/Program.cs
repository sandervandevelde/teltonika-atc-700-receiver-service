using Atc700Receiver;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
	options.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
	options.UseUtcTimestamp = true;
});
builder.Services.AddSingleton<KafkaJsonForwarder>();
builder.Services.AddHostedService<UdpReceiverService>();
builder.Build().Run();
