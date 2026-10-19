using Demo.Api.Services;
using Demo.Shared;
using Microsoft.AspNetCore.Mvc;

namespace Demo.Api.Controllers
{
    [ApiController]                          // 👈 ADD THIS
    [Route("api/[controller]")]              // 👈 ADD THIS
    public class OrdersController : ControllerBase
    {
        private readonly RabbitMqPublisher _publisher;
        public OrdersController(RabbitMqPublisher publisher) => _publisher = publisher;
        [HttpPost]
        public async Task<IActionResult> CreateOrder([FromBody] OrderMessage order)
        {
            order.OrderId = Guid.NewGuid();
            // 1. (Real world: Save to DB here)
            // 2. Publish to Queue
            await _publisher.PublishOrderAsync(order);
            // 3. Return immediately (Fast response)
            return Accepted(new { Message = "Order queued for processing", OrderId = order.OrderId });
        }
    }
}
