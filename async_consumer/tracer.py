from decimal import Decimal
import uuid
from datetime import date, datetime, timezone
from typing import Optional, Dict, Any
import aio_pika


class Span:
    
    def __init__(self, name: str, trace_id: str, parent_id: Optional[str] = None):
        self.name = name
        self.trace_id = trace_id
        self.span_id = self._generate_id()
        self.parent_id = parent_id
        self.start_time = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
        self.attributes: Dict[str, Any] = {}
    
    def _generate_id(self) -> str:
        return format(uuid.uuid4().int, 'x')[:16]
    
    def end(self):
        self.end_time = datetime.now(timezone.utc)
    
    def add_attribute(self, key: str, value: Any):
        self.attributes[key] = value
    
    def duration_ms(self) -> Optional[float]:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return None
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'trace_id': self.trace_id,
            'span_id': self.span_id,
            'parent_id': self.parent_id,
            'duration_ms': self.duration_ms(),
            'attributes': self.attributes
        }


class Tracer:
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.spans: list[Span] = []
    
    def parse_traceparent(self, traceparent: Optional[str]) -> Optional[Dict]:
        if not traceparent:
            return None
        
        parts = traceparent.split('-')
        if len(parts) != 4:
            return None
        
        return {
            'version': parts[0],
            'trace_id': parts[1],
            'parent_id': parts[2],
            'flags': parts[3]
        }
    
    def create_traceparent(self, trace_id: str, span_id: str) -> str:
        return f"00-{trace_id}-{span_id}-01"
    
    def start_span(self, name: str, incoming_traceparent: Optional[str] = None) -> Span:
        
        if incoming_traceparent:
            trace_info = self.parse_traceparent(incoming_traceparent)
            if trace_info:
                trace_id = trace_info['trace_id']
                parent_id = trace_info['parent_id']
            else:
                trace_id = self._generate_trace_id()
                parent_id = None
        else:
            trace_id = self._generate_trace_id()
            parent_id = None
        
        span = Span(name, trace_id, parent_id)
        span.add_attribute('service', self.service_name)
        
        print(f"Начало: {name}")
        print(f"  trace_id: {trace_id}")
        print(f"  span_id: {span.span_id}")
        print(f"  parent_id: {parent_id}")
        
        return span
    
    def _generate_trace_id(self) -> str:
        return format(uuid.uuid4().int, 'x')[:32]
    
    def end_span(self, span: Span):
        span.end()
        self.spans.append(span)
        
        print(f"Конец: {span.name} ({span.duration_ms():.2f}ms)")
        
    def _to_string(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        
        if isinstance(value, (bytes, bytearray)):
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                return value.hex()
        
        if isinstance(value, str):
            return value
        
        if isinstance(value, (int, float)):
            return str(value)
        
        if isinstance(value, Decimal):
            return str(value)
        
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        
        if hasattr(value, '__str__'):
            return str(value)
        
        return repr(value)
    
    def get_trace_context_from_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> Optional[str]:
        if message.headers:
            return self._to_string(message.headers.get('traceparent'))
        return None
    
    def add_trace_context_to_message(self, 
        message: aio_pika.Message, 
        span: Span,
        additional_headers: Optional[Dict] = None
        ) -> aio_pika.Message:
        
        headers = dict(message.headers) if message.headers else {}
        
        headers['traceparent'] = self.create_traceparent(span.trace_id, span.span_id)
        
        headers['tracestate'] = f"service={self.service_name}"
        
        if additional_headers:
            headers.update(additional_headers)
        
        return aio_pika.Message(
            body=message.body,
            headers=headers,
            delivery_mode=message.delivery_mode,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            correlation_id=message.correlation_id,
            reply_to=message.reply_to,
            message_id=message.message_id,
            timestamp=message.timestamp,
            expiration=message.expiration
        )