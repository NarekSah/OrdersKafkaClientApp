using OrdersKafkaWebApp.Components;
using Serilog;
using Serilog.Events;

namespace OrdersKafkaWebApp
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add Application Insights
            builder.Services.AddApplicationInsightsTelemetry();

            // Configure Serilog with Application Insights
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
                .MinimumLevel.Override("Microsoft.Hosting", LogEventLevel.Information)
                .MinimumLevel.Override("OrdersKafkaWebApp", LogEventLevel.Information)
                .MinimumLevel.Override("OrdersKafkaWebApp.Consumer", LogEventLevel.Debug)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .WriteTo.Debug()
                .WriteTo.File("logs/OrdersKafkaWebApp-.log", rollingInterval: RollingInterval.Day)
                .WriteTo.ApplicationInsights(
                    builder.Configuration.GetConnectionString("ApplicationInsights") ?? 
                    builder.Configuration["ApplicationInsights:ConnectionString"], 
                    TelemetryConverter.Traces)
                .CreateLogger();

            // Add services to the container.
            builder.Host.UseSerilog();
            
            builder.Services.AddRazorComponents()
                .AddInteractiveServerComponents();

            // Kafka consumer service (from referenced project)
            builder.Services.AddSingleton<IConsumer, Consumer>();

            var app = builder.Build();

            // Get logger for application startup
            var logger = app.Services.GetRequiredService<ILogger<Program>>();
            logger.LogInformation("Starting OrdersKafkaWebApp application");

            // Configure the HTTP request pipeline.
            if (!app.Environment.IsDevelopment())
            {
                app.UseExceptionHandler("/Error");
                app.UseHsts();
                logger.LogInformation("Application running in production mode");
            }
            else
            {
                logger.LogInformation("Application running in development mode");
            }

            app.UseHttpsRedirection();
            app.UseStaticFiles();
            app.UseAntiforgery();

            app.MapRazorComponents<App>()
                .AddInteractiveServerRenderMode();

            logger.LogInformation("Application configuration completed, starting web host");
            app.Run();
        }
    }
}
