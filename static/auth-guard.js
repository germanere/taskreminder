/* ============================================================
   AUTH GUARD — bắt buộc đăng nhập mới vào được trang
   ============================================================
   CÁCH DÙNG: dán dòng này vào NGAY ĐẦU <head>, TRƯỚC mọi CSS/JS khác,
   ở TẤT CẢ các trang cần bảo vệ (trang chủ, volume.html, analysis.html...):

   <head>
     <script src="/auth-guard.js"></script>
     ... (các thẻ khác giữ nguyên)
   </head>

   KHÔNG dán vào: /login.html, /register.html (2 trang này phải vào
   được khi CHƯA đăng nhập, nếu dán vào sẽ bị redirect vòng lặp vô hạn)
   ============================================================ */

(function () {
  // 1. Ẩn ngay nội dung trang để tránh "nháy" lộ ra trước khi kiểm tra xong
  document.documentElement.style.visibility = "hidden";

  var token = localStorage.getItem("mh_access_token");

  // 2. Chưa có token -> redirect ngay lập tức, không cần gọi API
  if (!token) {
    window.location.replace("/login.html");
    return;
  }

  // 3. Có token -> xác minh thật với server (phòng trường hợp token giả/hết hạn)
  fetch("/api/auth/me", {
    headers: { "Authorization": "Bearer " + token },
  })
    .then(function (res) {
      if (!res.ok) {
        // Token không hợp lệ / hết hạn -> xóa sạch, đá về login
        localStorage.removeItem("mh_access_token");
        localStorage.removeItem("mh_refresh_token");
        localStorage.removeItem("mh_user_email");
        window.location.replace("/login.html");
        return;
      }
      // Token hợp lệ -> cho hiện trang
      document.documentElement.style.visibility = "visible";
    })
    .catch(function () {
      // Lỗi kết nối -> an toàn là chặn luôn, tránh cho vào khi không xác minh được
      window.location.replace("/login.html");
    });
})();
