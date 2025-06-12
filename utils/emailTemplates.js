// utils/emailTemplates.js

exports.welcomeTemplate = (username) => `
  <div style="font-family: Arial, sans-serif;">
    <h1>Welcome, ${username}!</h1>
    <p>Thank you for registering at AgentAI. You can now log in and start scheduling your AI-powered social posts.</p>
  </div>
`;

exports.resetPasswordTemplate = (resetUrl) => `
  <div style="font-family: Arial, sans-serif;">
    <h1>Password Reset Request</h1>
    <p>Click <a href="${resetUrl}">here</a> to reset your password. This link will expire in 1 hour.</p>
  </div>
`;

exports.subscriptionConfirmationTemplate = (planName, expiryDate) => `
  <div style="font-family: Arial, sans-serif;">
    <h1>Subscription Confirmed</h1>
    <p>Your <strong>${planName}</strong> plan is now active until <strong>${expiryDate}</strong>.</p>
    <p>Thank you for choosing AgentAI!</p>
  </div>
`;
