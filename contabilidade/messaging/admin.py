from django.contrib import admin

from .models import MessageQueue, MessageTemplate


@admin.register(MessageTemplate)
class MessageTemplateAdmin(admin.ModelAdmin):
    list_display = ("name",)


@admin.register(MessageQueue)
class MessageQueueAdmin(admin.ModelAdmin):
    list_display = ("client", "status", "attempts", "created_at", "sent_at")
    list_filter = ("status",)
    search_fields = ("client__name", "final_text")
