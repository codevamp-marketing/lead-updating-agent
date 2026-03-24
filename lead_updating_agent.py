import asyncio
import json
import os
import select
from datetime import datetime

import psycopg2
from supabase import create_client

# ── ENV CONFIG ─────────────────────────────────────────────
SUPABASE_URL = "https://rthwmoayizwgfqcsbsip.supabase.co"
SUPABASE_SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ0aHdtb2F5aXp3Z2ZxY3Nic2lwIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjAwNTQ3NiwiZXhwIjoyMDg3NTgxNDc2fQ.Ajhbdom6w4cLFK8WdY-jtAcivV98UVXki-U7IEueXOQ"



supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("[✓] Supabase Connected")

# ════════════════════════════════════════════════════════════
#  ACTIVITY SCORE CONFIG
# ════════════════════════════════════════════════════════════

ACTIVITY_SCORE = {
    "Call": 20,
    "Meeting": 40,
    "Email": 10,
    "Campaign Interaction": 5,
    "Status Change": 5,
    "WhatsApp": 15   # ✅ fixed comma bug
}

# ════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════

def get_priority(score: int) -> str:
    if score >= 70:
        return "High"
    elif score >= 40:
        return "Medium"
    else:
        return "Low"


def get_next_action(priority: str) -> str:
    return {
        "High": "Book demo / Close deal",
        "Medium": "Follow-up call",
        "Low": "Nurture via email"
    }[priority]


# ════════════════════════════════════════════════════════════
#  PROCESS ACTIVITY
# ════════════════════════════════════════════════════════════

async def process_activity(activity: dict):
    try:
        lead_id = activity.get("lead_id")
        activity_type = activity.get("type")
        description = (activity.get("description") or "").lower()

        print(f"\n[+] Activity: {activity_type} | Lead ID: {lead_id}")

        # Fetch lead
        lead = (
            supabase.table("Lead")
            .select("score")
            .eq("id", lead_id)
            .single()
            .execute()
            .data
        )

        if not lead:
            print("    ⚠ Lead not found")
            return

        old_score = lead.get("score", 0)

        # ── Base Score Update ─────────────────────────────
        score_change = ACTIVITY_SCORE.get(activity_type, -10)
        new_score = old_score + score_change

        # ── Intent-based scoring 🔥 ───────────────────────
        if "interested" in description:
            new_score += 30
        elif "no response" in description:
            new_score -= 20
        elif "converted" in description:
            new_score += 50

        # Clamp score
        new_score = max(0, min(new_score, 100))

        # ── Priority ──────────────────────────────────────
        priority = get_priority(new_score)
        next_action = get_next_action(priority)

        # ── Status Logic 🔥 ───────────────────────────────
        if "converted" in description:
            status = "Customer"
        elif "interested" in description:
            status = "Interested"
        elif "no response" in description:
            status = "Cold"
        else:
            if new_score >= 70:
                status = "Hot"
            elif new_score >= 40:
                status = "Warm"
            else:
                status = "Cold"

        # ── Update Lead ───────────────────────────────────
        supabase.table("Lead").update({
            "score": new_score,
            "priority": priority,
            "nextBestAction": next_action,
            "status": status,  
            "lastInteraction": datetime.utcnow().isoformat()
        }).eq("id", lead_id).execute()

        # ── Mark Activity Processed ───────────────────────
        supabase.table("activities").update({
            "processed": True
        }).eq("id", activity.get("id")).execute()

        print(
            f"    → Score: {old_score} → {new_score} "
            f"| {priority} | Status: {status}"
        )

    except Exception as e:
        print(f"    ✗ Error: {e}")


# ════════════════════════════════════════════════════════════
#  LISTEN LOOP (REAL-TIME)
# ════════════════════════════════════════════════════════════

async def listen_loop():
    loop = asyncio.get_event_loop()

    print("[*] Connecting to Postgres...")

    conn = await loop.run_in_executor(
        None,
        lambda: psycopg2.connect(DATABASE_URL)
    )

    conn.set_isolation_level(0)
    cur = conn.cursor()

    cur.execute("LISTEN new_activity;")

    print("[✓] Listening on channel: new_activity")
    print("[*] Waiting for activities...\n")

    while True:
        try:
            await loop.run_in_executor(
                None,
                lambda: select.select([conn], [], [], 5)
            )

            conn.poll()

            while conn.notifies:
                notify = conn.notifies.pop(0)

                try:
                    activity = json.loads(notify.payload)
                    await process_activity(activity)

                except Exception as e:
                    print(f"[ERROR] {e}")

        except psycopg2.OperationalError as e:
            print(f"[!] Connection lost: {e}")
            print("[*] Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


# ════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════

async def main():
    print("=" * 60)
    print("  F2Fintech — Activity Score Agent (AI + STATUS)")
    print("=" * 60)

    try:
        rows = supabase.table("activities").select("id").limit(1).execute()
        print(f"[✓] Supabase OK (rows: {len(rows.data or [])})")
    except Exception as e:
        print(f"[!] Supabase error: {e}")

    await listen_loop()


# ════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Stopped by user")
    except Exception as e:
        print(f"\n[FATAL] {e}")