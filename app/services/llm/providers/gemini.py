"""Google Gemini LLM provider implementation."""
import warnings
import logging
from typing import Optional
import os
from .base import (
    BaseLLMProvider,
    LLMProviderError,
    LLMProviderTimeoutError,
    LLMProviderConnectionError,
    LLMProviderAuthenticationError
)

logger = logging.getLogger(__name__)

# Suppress deprecation warning for google.generativeai
warnings.filterwarnings("ignore", message=".*google.generativeai.*", category=FutureWarning)

# Configure SSL certificates for Gemini API (fixes CERTIFICATE_VERIFY_FAILED)
try:
    import certifi
    # Set SSL certificate file for httpx/requests (used by google-generativeai)
    if not os.getenv('SSL_CERT_FILE'):
        os.environ['SSL_CERT_FILE'] = certifi.where()
    if not os.getenv('REQUESTS_CA_BUNDLE'):
        os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
except ImportError:
    pass  # certifi not installed, use system defaults

# Try to import Google Generative AI, but make it optional
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logger.warning("Google Generative AI library not installed. Install with: pip install google-generativeai")


class GeminiProvider(BaseLLMProvider):
    """Google Gemini provider using Generative AI API."""
    
    def __init__(
        self,
        model: str = "gemini-pro",
        timeout: int = 120,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        api_key: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Gemini provider.
        
        Args:
            model: Gemini model name (e.g., "gemini-pro", "gemini-pro-vision")
            timeout: Request timeout in seconds
            temperature: Sampling temperature (0.0-2.0)
            max_tokens: Maximum tokens to generate
            api_key: Google API key (defaults to GEMINI_API_KEY env var)
            **kwargs: Additional Gemini-specific config
        """
        if not GEMINI_AVAILABLE:
            raise ImportError(
                "Google Generative AI library not installed. Install with: pip install google-generativeai"
            )
        
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        
        # Configure Gemini client before validation
        self.client = None
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.client = genai.GenerativeModel(model)
        
        # Now call super().__init__ which will call _validate_config()
        super().__init__(model, timeout, temperature, max_tokens, **kwargs)
    
    def _validate_config(self) -> None:
        """Validate Gemini configuration."""
        if not self.model:
            raise ValueError("Gemini model name is required")
        
        if not self.api_key:
            raise LLMProviderAuthenticationError(
                "gemini",
                "API key is required. Set GEMINI_API_KEY environment variable or pass api_key parameter."
            )
        
        if not self.client:
            raise LLMProviderConnectionError(
                "gemini",
                self.model,
                "Failed to initialize Gemini client"
            )
    
    def generate(self, prompt: str, **kwargs) -> str:
        """
        Generate text using Gemini API.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional parameters (safety_settings, generation_config, etc.)
            
        Returns:
            Generated text response
        """
        try:
            logger.info(f"Gemini generating with model: {self.model}")
            
            # Prepare generation config
            generation_config_dict = {
                "temperature": kwargs.get("temperature", self.temperature),
            }
            
            if self.max_tokens:
                generation_config_dict["max_output_tokens"] = self.max_tokens
            elif "max_tokens" in kwargs:
                generation_config_dict["max_output_tokens"] = kwargs["max_tokens"]
            
            # Create generation config object
            generation_config = genai.types.GenerationConfig(**generation_config_dict)
            
            # Prepare request parameters
            request_params = {
                "contents": prompt,
                "generation_config": generation_config
            }
            
            # Add provider-specific parameters
            if "safety_settings" in kwargs:
                request_params["safety_settings"] = kwargs["safety_settings"]
            
            # Generate content
            response = self.client.generate_content(**request_params)
            
            # Extract text from response
            if not response:
                raise LLMProviderError("Gemini returned empty response")
            
            # Handle different response formats
            if hasattr(response, 'text') and response.text:
                output = response.text
            elif hasattr(response, 'candidates') and response.candidates:
                # Handle candidate-based responses
                candidate = response.candidates[0]
                if hasattr(candidate, 'content') and hasattr(candidate.content, 'parts'):
                    output = ''.join(part.text for part in candidate.content.parts if hasattr(part, 'text'))
                else:
                    output = str(candidate)
            else:
                output = str(response)
            
            if not output or not output.strip():
                raise LLMProviderError("Gemini returned empty response")
            
            logger.info(f"Gemini returned output (length: {len(output)}, first 200 chars: {output[:200]})")
            return output.strip()
            
        except Exception as e:
            error_msg = str(e)
            if "API_KEY" in error_msg or "authentication" in error_msg.lower():
                raise LLMProviderAuthenticationError("gemini", error_msg)
            elif "timeout" in error_msg.lower():
                raise LLMProviderTimeoutError(self.timeout, "gemini", self.model)
            elif "connection" in error_msg.lower() or "network" in error_msg.lower():
                raise LLMProviderConnectionError("gemini", self.model, error_msg)
            else:
                logger.error(f"Unexpected Gemini error: {error_msg}")
                raise LLMProviderConnectionError("gemini", self.model, error_msg)
    
    def get_provider_name(self) -> str:
        """Return provider identifier."""
        return "gemini"

