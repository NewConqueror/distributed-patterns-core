local stock = redis.call('GET', KEYS[1])
if not stock then
    return -1 -- Ürün bulunamadı
end
if tonumber(stock) <= 0 then
    return 0 -- Stok bitti
end
redis.call('DECR', KEYS[1])
return 1 -- Başarılı
