from openai import OpenAI, APIError, RateLimitError
import re, json, tiktoken, concurrent.futures, time, threading

# --- Add once, near the top ---------------------------------------------------
# Cost per-1M tokens in USD (2025-07-02 price list; change if OpenAI updates)
_PRICES = {
    "gpt-4o":               (2.5, 10),     # (prompt, completion)
    "gpt-4o-mini":          (0.15, 0.6),
    "o3":                   (2, 8),
    "o3-mini":              (1.1, 4.4),
    # add other models you use here …
}
# -----------------------------------------------------------------------------

class GPT:
    def __init__(self, api_key, api_base=None, model="gpt-4o"):
        self.api_base = api_base
        self.api_key = api_key
        self.model = model
        
        # ---- running totals --------------------------------------------------
        self.total_prompt_tokens      = 0
        self.total_completion_tokens  = 0
        self.total_dollars            = 0.0
        # ---------------------------------------------------------------------

        self._lock = threading.Lock()      # guards the four totals above

    def invoke_GPT_in_parallel(self, prompts, json_format=True):
        completions = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.get_GPT_response_json, prompt, json_format): prompt for prompt in prompts}
            for future in concurrent.futures.as_completed(futures):
                completions.append(future.result())

        return completions

    def get_GPT_response_json(self, prompt, json_format=True): # This function returns the GPT response, which can be specified to return json or string format
        client = (OpenAI(api_key=self.api_key, base_url=self.api_base) if self.api_base 
                    else OpenAI(api_key=self.api_key))
        
        # reasoning model
        if self.model in ["o1-preview", "o1-mini", "o3-mini", "o4-mini"]:
            try:
                response = client.chat.completions.create(
                    model = self.model,
                    messages = [
                        {
                            "role": "developer",
                            "content": [
                                {
                                "type": "text",
                                "text": "You are an experienced Database Administrator (DBA) and you will create high-quality SQL templates."
                                }
                            ]
                        },
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": prompt
                                },
                            ],
                        }
                    ],
                    response_format={
                        "type": "json_object"
                    },
                    reasoning_effort="medium",
                    store=False
                )

            except RateLimitError as e:
                wait_time = float(e.response.headers.get('Retry-After', 0.5))
                print(f"Error: {e}. Rate limit hit. Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                return self.get_GPT_response_json(prompt, json_format=True)
            
        else:  
            if json_format: # json
                try:
                    response = client.chat.completions.create(
                        messages=[
                            {"role": "system", "content": "You should output JSON."},
                            {'role':'user', 'content':prompt}],
                        model=self.model, 
                        response_format={"type": "json_object"}, 
                        temperature=0.1,
                    )
            
                except RateLimitError as e:
                    wait_time = float(e.response.headers.get('Retry-After', 0.5))
                    print(f"Rate limit hit. Waiting for {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                    return self.get_GPT_response_json(prompt, json_format=True)
                         
            else: # string
                try: 
                    response = client.chat.completions.create(
                        messages=[
                            {'role':'user', 'content':prompt}],
                        model=self.model, 
                        temperature=0.1,     
                    )
                except RateLimitError as e:
                    wait_time = float(e.response.headers.get('Retry-After', 0.5))
                    print(f"Rate limit hit. Waiting for {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                    return self.get_GPT_response_json(prompt, json_format=True)
                
        # ③  pull usage numbers returned by the API ---------------------------
        usage = response.usage              # safe: OpenAI always returns this
        p_tok = usage.prompt_tokens
        c_tok = usage.completion_tokens

        # ④  compute call-level cost ------------------------------------------
        in_rate, out_rate = _PRICES.get(self.model, (0, 0))
        call_cost = (p_tok * in_rate + c_tok * out_rate) / 1000000.0

        # ⑤  add to the running totals (thread-safe) --------------------------
        with self._lock:
            self.total_prompt_tokens     += p_tok
            self.total_completion_tokens += c_tok
            self.total_dollars           += call_cost

        # ⑥  prepare the return value exactly as you did before --------------
        raw = response.choices[0].message.content
        return json.loads(raw) if json_format else raw

    def remove_html_tags(self, text):
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)
    
    def extract_json_from_text(self, text):
        json_pattern = r'\{[^{}]*\}'
        match = re.search(json_pattern, text)
        if match:
            try:
                json_data = json.loads(match.group())
                return json_data
            except json.JSONDecodeError:
                return None
        else:
            return None