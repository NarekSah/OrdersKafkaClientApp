using Microsoft.AspNetCore.Mvc;

namespace OrdersKafkaClientApp.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class OrdersController : ControllerBase
    {
        private readonly IProducer _producer;

        public OrdersController(IProducer producer)
        {
            _producer = producer;
        }

        [HttpPost]
        public async Task<IActionResult> CreateOrder([FromBody] OrderMessage orderMessage)
        {
            if (orderMessage == null)
            {
                return BadRequest("Order message cannot be null");
            }

            if (string.IsNullOrEmpty(orderMessage.Key))
            {
                return BadRequest("Order key cannot be null or empty");
            }

            try
            {
                await _producer.ProduceAsync("orders", orderMessage);
                return Ok(new { Message = "Order message sent to Kafka successfully", Key = orderMessage.Key });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Failed to send order message to Kafka", Error = ex.Message });
            }
        }
    }
}
