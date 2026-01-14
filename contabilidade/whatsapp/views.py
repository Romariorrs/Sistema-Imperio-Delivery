import threading
import time

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseNotAllowed
from django.shortcuts import render
from django.utils import timezone

from contabilidade.messaging.models import MessageQueue

from .session import get_client, reset_client
from .services import WhatsAppError

_worker_running = False


@login_required
def whatsapp_status(request):
    return render(request, "whatsapp/status.html")


@login_required
def whatsapp_status_json(request):
    try:
        client = get_client()
        status = client.get_connection_status()
        qr_base64 = None
        if status == "qr":
            qr_base64 = client.get_qr_screenshot_base64()
        return JsonResponse({"status": status, "qr_base64": qr_base64})
    except Exception as exc:
        reset_client()
        return JsonResponse({"status": "error", "error": str(exc)}, status=500)


def _process_queue(delay: float = 2.0):
    global _worker_running
    try:
        pending = MessageQueue.objects.filter(status__in=["pending", "error"]).order_by("created_at")
        reset_client()
        client = get_client()
        for msg in pending:
            try:
                client.send_message(msg.client.phone, msg.final_text)
                msg.status = "sent"
                msg.sent_at = timezone.now()
                msg.error_message = ""
            except WhatsAppError as exc:
                msg.status = "error"
                msg.error_message = str(exc)
                msg.sent_at = timezone.now()
            msg.attempts += 1
            msg.save()
            time.sleep(delay)
    finally:
        _worker_running = False


@login_required
def whatsapp_send_now(request):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    global _worker_running
    if _worker_running:
        return JsonResponse({"started": False, "message": "Processo já em execução"})
    _worker_running = True
    delay = float(request.POST.get("delay", 2.0))
    threading.Thread(target=_process_queue, args=(delay,), daemon=True).start()
    return JsonResponse({"started": True, "message": "Envio iniciado"})


@login_required
def whatsapp_reset(request):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    reset_client()
    return JsonResponse({"reset": True})


@login_required
def whatsapp_worker_status(request):
    return JsonResponse({"running": _worker_running})


@login_required
def whatsapp_start_visible(request):
    """Retorna QR via Evolution API (se disponível)."""
    reset_client()
    client = get_client()
    status = client.get_connection_status()
    qr_base64 = None
    if status == "qr":
        qr_base64 = client.get_qr_screenshot_base64()
    return JsonResponse({"status": status, "qr_base64": qr_base64})
