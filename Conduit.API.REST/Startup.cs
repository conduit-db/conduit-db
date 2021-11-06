using Conduit.API.REST.Classes;
using Conduit.API.REST.Services;
using Conduit.MySQL;
using Conduit.MySQL.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Conduit.API.REST
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddTransient(_ => new ApplicationDatabase(Configuration["ConnectionStrings:DefaultConnection"]));
            services.AddTransient<IHeaderService, HeaderService>();
            services.AddTransient<INetworkService, NetworkService>();
            services.AddTransient<IRestorationService, RestorationService>();
            services.AddTransient<IScanService, ScanService>();
            services.AddTransient<ITransactionService, TransactionService>();
            services.AddHostedService<HeaderHostedService>();
            services.AddControllers();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "Conduit.API.REST", Version = "v1" });
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseSwagger();
                app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "Conduit.API.REST v1"));
            }
            else
            {
                // NOTE(rt) For now only require HTTPS for production.
                app.UseHttpsRedirection();
            }

            app.UseRouting();

            app.UseAuthorization();
            app.UseWebSockets();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
