local stock = redis.call('GET', KEYS[1])
if not stock then
    return -1 -- Ürün bulunamadı
end
if tonumber(stock) <= 0 then
    return 0 -- Stok bitti
end

-- 1. Atomik stok düşümü
redis.call('DECR', KEYS[1])

-- 2. Atomik olarak Redis Stream'e rezervasyon mesajı yaz (Dual-Write problemini sıfırlar!)
redis.call('XADD', KEYS[2], '*', 
    'reservation_id', ARGV[1], 
    'product_id', ARGV[2], 
    'user_id', ARGV[3], 
    'created_at', ARGV[4]
)

return 1 -- Başarılı
