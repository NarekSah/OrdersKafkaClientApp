using Microsoft.AspNetCore.Mvc.RazorPages;

using SimpleKafkaWeb.Services;

namespace SimpleKafkaWeb.Pages
{
    public class IndexModel : PageModel
    {
        private readonly MessageStore _store;

        public IReadOnlyCollection<string> Messages { get; private set; } = Array.Empty<string>();

        public IndexModel(MessageStore store)
        {
            _store = store;
        }

        public void OnGet()
        {
            Messages = _store.GetMessages();
        }
    }
}
