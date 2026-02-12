# Gmail App Password Setup Guide

## Step 1: Enable 2-Factor Authentication

1. Go to [Google Account Security](https://myaccount.google.com/security)
2. Scroll to "How you sign in to Google"
3. Click **2-Step Verification** → Follow prompts to enable

> **Note:** You must have 2FA enabled before you can create app passwords.

## Step 2: Generate App Password

1. Visit [App Passwords](https://myaccount.google.com/apppasswords)
   - Or: Google Account → Security → 2-Step Verification → App passwords (at bottom)

2. **Select app:** Mail

3. **Select device:** Other (Custom name)
   - Enter: `Market Pulse` or `Sarasota Pipeline`

4. Click **Generate**

5. **Copy the 16-character password**
   - Example: `abcd efgh ijkl mnop`
   - **Save this** - you won't see it again!

## Step 3: Add to GitHub Secrets

1. Go to your repo on GitHub
2. Navigate to: **Settings → Secrets and variables → Actions**
3. Click **New repository secret**

Add these **three secrets**:

| Name | Value |
|------|-------|
| `GMAIL_USER` | Your Gmail address (e.g., `yourname@gmail.com`) |
| `GMAIL_APP_PASSWORD` | The 16-character password from Step 2 |
| `EMAIL_TO` | Recipient email (can be same as GMAIL_USER) |

## Step 4: Test Locally (Optional)

```bash
# Set environment variables
export GMAIL_USER="yourname@gmail.com"
export GMAIL_APP_PASSWORD="abcd efgh ijkl mnop"  # Remove spaces
export EMAIL_TO="recipient@example.com"

# Run pipeline
python main.py
```

## Troubleshooting

### Error: "Username and Password not accepted"
- Verify 2FA is enabled
- Regenerate app password (old one may be invalid)
- Remove spaces from app password: `abcdefghijklmnop`

### Error: "SMTP AUTH extension not supported"
- Make sure you're using `smtp.gmail.com` on port `465` (SSL)
- The code already uses `SMTP_SSL` - no changes needed

### Email not received
- Check spam folder
- Verify `EMAIL_TO` is correct
- Check GitHub Actions logs for errors

### Rate Limits
- Gmail allows ~500 emails/day for free accounts
- Daily pulse = 1 email/day = well within limits

## Security Notes

✅ **App passwords are scoped** - can only send mail, can't read inbox  
✅ **Revocable anytime** - Just delete in Google Account settings  
✅ **No regular password exposure** - Never share your main Google password  

---

**Ready to deploy!** Just add the three GitHub secrets and enable Actions.
