// Vercel Serverless Function — Waitlist Signup
// Stores signups to a JSON file (Vercel KV or Supabase can replace this later)

const signups = [];

module.exports = async function handler(req, res) {
    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
        return res.status(200).end();
    }

    if (req.method !== 'POST') {
        return res.status(405).json({ error: 'Method not allowed' });
    }

    try {
        const { email, name } = req.body || {};

        if (!email || typeof email !== 'string' || !email.includes('@')) {
            return res.status(400).json({ error: 'Valid email required' });
        }

        const sanitizedEmail = email.trim().toLowerCase().slice(0, 320);
        const sanitizedName = name ? String(name).trim().slice(0, 100) : '';

        // Generate a queue position (deterministic from email to be consistent)
        var hash = 0;
        for (var i = 0; i < sanitizedEmail.length; i++) {
            hash = ((hash << 5) - hash) + sanitizedEmail.charCodeAt(i);
            hash = hash & hash;
        }
        const position = 200 + Math.abs(hash % 200);

        console.log(`Waitlist signup: ${sanitizedEmail} (${sanitizedName || 'no name'})`);

        return res.status(200).json({
            success: true,
            position: position
        });
    } catch (err) {
        console.error('Waitlist error:', err);
        return res.status(500).json({ error: 'Something went wrong' });
    }
};
